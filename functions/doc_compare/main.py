"""
index.py — Zoho Catalyst Advanced I/O Function
===============================================
Complete drop-in replacement for app.py (Cloud Run / FastAPI).

WHAT CHANGED vs app.py
───────────────────────
1. No FastAPI / uvicorn — Catalyst uses handler(request).
2. No OAuth token management — context.get_token() returns a live token
   (context = zcatalyst_sdk.initialize(), NOT request.catalyst — that
   attribute doesn't exist on the Request object).
3. No in-memory jobs={} — Catalyst DataStore table "CompareJobs" replaces it.
4. No background thread — Catalyst runs synchronously up to 540s.
   The pipeline runs end-to-end in handle_analyze(); DataStore writes
   provide live progress so the widget's poll loop sees each phase.
5. No auto-cancel watcher thread — not needed; Catalyst enforces its own timeout.
6. is_cancelled() reads DataStore instead of the in-memory dict.

EVERYTHING ELSE IS IDENTICAL to app.py:
  - All Zoho API calls (same URLs, same logic)
  - Prompt loading from AIPrompts CRM module (5-min cache preserved)
  - format_zoho_quote, extract_pdf_gemini, run_comparison, generate_pdf_report
  - attach_pdf_to_quote, attach_via_filestore
  - Margin gate, subtotal validation
  - WeasyPrint PDF with full HTML template

DATASTORE SETUP (do this once in Catalyst Console)
────────────────────────────────────────────────────
Table name : CompareJobs
Columns    :
  job_id        — Single Line   (job UUID — use as lookup key)
  status        — Single Line   (processing | done | error | cancelled)
  phase         — Single Line   (phase label shown in the widget)
  result_json   — Multi Line    (full Claude JSON blob)
  generated_at  — Single Line   (ISO timestamp)
  quote_ref     — Single Line   (Quotation_Reference from quote)
  error         — Multi Line    (error message when status=error)

ENV VARS (set in Catalyst Console → Functions → document_compare → Env Vars)
────────────────────────────────────────────────────────────────────────────
  CLAUDE_API_KEY   ← Anthropic key
  GEMINI_API_KEY   ← Google AI key
  PPO_PDF_FIELD    ← default: PARTNER_PO_PDF
  VQ_PDF_FIELD     ← default: VQ_PDF

  Do NOT add CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN — Catalyst handles auth.

WIDGET CHANGE (index.html — one line)
──────────────────────────────────────
  var BASE_URL = "https://<your-project>.catalystapps.com/server/document_compare";
"""

import json
import re
import os
import time
import uuid
import threading
from datetime import datetime

import anthropic
import requests
import zcatalyst_sdk
from flask import make_response, jsonify
import google.generativeai as genai
from io import BytesIO
from xhtml2pdf import pisa


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
PPO_PDF_FIELD  = os.environ.get("PPO_PDF_FIELD",  "PARTNER_PO_PDF")
VQ_PDF_FIELD   = os.environ.get("VQ_PDF_FIELD",   "VQ_PDF")
ZOHO_BASE_URL  = "https://www.zohoapis.com"

# Margin gate config — identical to app.py
OPPORTUNITY_FIELD_ON_QUOTE = "Deal_Name"
OPPORTUNITIES_MODULE       = "Deals"
OPPORTUNITIES_NAME_FIELD   = "Deal_Name"
GROSS_MARGIN_FIELD         = "GrossMarginCalc"
VENDORS_MODULE             = "Vendors"
VENDORS_NAME_FIELD         = "Name"
VENDOR_MARGIN_FIELD        = "Min_Acceptable_Margin"
AMOUNT_IN_USD_FIELD        = "Amount_in_USD"
NET_TO_VENDOR_FIELD        = "Net_to_Vendor"
SUBTOTAL_TOLERANCE_USD     = 1.00

# Prompt cache — same 5-min TTL as app.py
_PROMPT_CACHE_TTL = 300
_crm_prompt_cache = {"gemini": (None, 0), "claude": (None, 0)}

# Gemini model cache
_gemini_model_cache = None

# Fixed exchange rates — kept in sync with Claude prompt Step 3c
_USD_RATES = {"USD": 1.0, "AED": 1/3.6725, "SAR": 1/3.7500, "QAR": 1/3.6500}

# DataStore table name
_DS_TABLE = "CompareJobs"

# job_id is always a server-generated uuid4 string — validated before ZCQL interpolation
_JOB_ID_RE = re.compile(r'^[0-9a-fA-F-]{1,64}$')


# ═════════════════════════════════════════════════════════════
# CATALYST ENTRY POINT
# ═════════════════════════════════════════════════════════════
def handler(request):
    """
    Catalyst Advanced I/O handler — `request` is a Flask Request object.
    zcatalyst_sdk.initialize()  → Catalyst context (token, datastore, etc.)
    request.method       → HTTP method string
    request.url          → full request URL
    request.get_data()   → raw request body (bytes) — Flask Request has no .body
    """
    context = zcatalyst_sdk.initialize()
    path    = str(request.url or "")
    method  = (request.method or "GET").upper()
    body    = request.get_data(as_text=True) or ""

    try:
        if method == "POST" and "analyze-quote" in path:
            return _handle_analyze(context, body)

        if method == "GET" and "job-status" in path:
            job_id = path.split("job-status/")[-1].strip("/").split("?")[0]
            return _handle_job_status(context, job_id)

        if method == "GET" and "check-report" in path:
            quote_id = path.split("check-report/")[-1].strip("/").split("?")[0]
            return _handle_check_report(context, quote_id)

        if method == "POST" and "cancel-job" in path:
            job_id = path.split("cancel-job/")[-1].strip("/").split("?")[0]
            return _handle_cancel(context, job_id)

        return _resp(404, {"error": "Not found"})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return _resp(500, {"error": str(e)})


def _resp(status: int, body: dict):
    """Return a Flask-compatible response (Catalyst Advanced I/O uses Flask under the hood)."""
    return make_response(jsonify(body), status)


# ═════════════════════════════════════════════════════════════
# DATASTORE HELPERS  (replaces in-memory jobs={} from app.py)
# ═════════════════════════════════════════════════════════════
def _ds_write(context, job_id: str, data: dict):
    """
    Upsert a job row in the CompareJobs DataStore table.
    Catalyst rows are keyed by a system ROWID, not by our own job_id column, and
    Table has no filter-by-column update — so look the row up by job_id first (via
    ZCQL) and update by ROWID, or insert a new row if none exists yet.
    """
    try:
        table    = context.datastore().table(_DS_TABLE)
        existing = _ds_read(context, job_id)
        if existing.get("ROWID"):
            table.update_row({"ROWID": existing["ROWID"], **data})
        else:
            table.insert_row({"job_id": job_id, **data})
    except Exception as e:
        print(f"[DataStore] write error: {e}")


def _ds_read(context, job_id: str) -> dict:
    """
    Read a job row from CompareJobs by job_id.
    Table has no get_rows()/filter-by-column call — only get_row(ROWID) and
    get_paged_rows() — so a column lookup has to go through ZCQL instead.
    """
    if not job_id or not _JOB_ID_RE.match(job_id):
        return {}
    try:
        rows = context.zcql().execute_query(
            f"SELECT * FROM {_DS_TABLE} WHERE job_id = '{job_id}'"
        )
        return rows[0].get(_DS_TABLE, {}) if rows else {}
    except Exception as e:
        print(f"[DataStore] read error: {e}")
        return {}


def _ds_delete_old(context, max_age_hours: int = 24):
    """Housekeeping — delete rows older than max_age_hours to avoid table bloat."""
    try:
        table  = context.datastore().table(_DS_TABLE)
        cutoff = time.time() - max_age_hours * 3600
        for row in table.get_iterable_rows():
            ts = row.get("generated_at") or ""
            try:
                row_time = datetime.fromisoformat(ts).timestamp()
                if row_time < cutoff:
                    table.delete_row(row.get("ROWID"))
            except Exception:
                pass
    except Exception as e:
        print(f"[DataStore] cleanup error: {e}")


def _is_cancelled(context, job_id: str) -> bool:
    """Check if a job has been cancelled via DataStore."""
    return _ds_read(context, job_id).get("status") == "cancelled"


def _phase(context, job_id: str, phase: str):
    """Write current phase to DataStore so the widget poll loop sees it."""
    print(f"[{job_id}] {phase}")
    _ds_write(context, job_id, {"status": "processing", "phase": phase})


# ═════════════════════════════════════════════════════════════
# AUTH  (same refresh-token flow as the original Cloud Run app.py)
# ═════════════════════════════════════════════════════════════
# Catalyst's own context.credential.token() authenticates the caller to the
# CATALYST PROJECT (DataStore/Cache/Filestore) — confirmed live via Zoho's
# OAUTH_SCOPE_MISMATCH error that it carries no Zoho CRM API scope at all.
# There's no Catalyst-native way to get a CRM-scoped token for free, so this
# restores the same client_id/secret/refresh_token exchange app.py used.
CLIENT_ID     = os.environ.get("CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN", "")
ZOHO_ACCOUNTS_URL = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")

_zoho_token_cache = {"token": None, "expires_at": 0}


def _get_token(context) -> str:
    """
    Returns a live Zoho CRM OAuth access token, refreshing it via the standard
    refresh_token grant when the cached one is missing/expired. Cached at module
    level so a warm Catalyst worker reuses one token across invocations instead
    of hitting accounts.zoho.com on every call.
    """
    now = time.time()
    if _zoho_token_cache["token"] and now < _zoho_token_cache["expires_at"] - 60:
        return _zoho_token_cache["token"]

    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError(
            "CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN are not set "
            "(Catalyst Console → Functions → doc_compare → Env Vars). "
            "Catalyst's own credential has no Zoho CRM API scope, so these "
            "are required — see OAUTH_SCOPE_MISMATCH."
        )

    r = requests.post(f"{ZOHO_ACCOUNTS_URL}/oauth/v2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=20)
    data = r.json()
    if "access_token" not in data:
        raise RuntimeError(f"Zoho token refresh failed: {data}")

    _zoho_token_cache["token"]      = data["access_token"]
    _zoho_token_cache["expires_at"] = now + data.get("expires_in", 3600)
    print(f"[auth] Refreshed Zoho CRM token, expires in {data.get('expires_in', 3600)}s")
    return _zoho_token_cache["token"]


# ═════════════════════════════════════════════════════════════
# PARALLELISM  (raw threading.Thread — NOT concurrent.futures.ThreadPoolExecutor)
# ═════════════════════════════════════════════════════════════
def _run_parallel(*tasks):
    """
    Run zero-arg callables concurrently on real OS threads and return their
    results in the same order, re-raising the first exception if any failed.

    Deliberately NOT concurrent.futures.ThreadPoolExecutor: that module keeps a
    single process-wide atexit hook shared by every executor in the process.
    Once it fires (real interpreter shutdown, e.g. a prior/abandoned invocation's
    worker teardown on a platform that reuses warm processes), it permanently
    breaks .submit() on every ThreadPoolExecutor for the rest of that process's
    life — "cannot schedule new futures after interpreter shutdown" — even for
    an unrelated new request. Plain threading.Thread has no such shared flag,
    so it gives the same real parallelism without that fragility.
    """
    results = [None] * len(tasks)
    errors  = [None] * len(tasks)

    def _runner(i, fn):
        try:
            results[i] = fn()
        except Exception as e:
            errors[i] = e

    threads = [threading.Thread(target=_runner, args=(i, fn)) for i, fn in enumerate(tasks)]
    for t in threads: t.start()
    for t in threads: t.join()

    for e in errors:
        if e is not None:
            raise e
    return results


# ═════════════════════════════════════════════════════════════
# ENDPOINTS
# ═════════════════════════════════════════════════════════════
def _handle_analyze(context, body: str):
    body         = json.loads(body or "{}")
    quote_id     = body.get("quote_id")
    initiated_by = body.get("initiated_by", "")

    if not quote_id:
        return _resp(400, {"error": "quote_id missing"})

    job_id = str(uuid.uuid4())
    _ds_write(context, job_id, {
        "status": "processing",
        "phase":  "Initialising..."
    })

    # Run housekeeping in background (best-effort, non-blocking)
    try:
        _ds_delete_old(context)
    except Exception:
        pass

    try:
        token = _get_token(context)

        # ── Validate prompts first (fail fast) ───────────────
        load_gemini_prompt(token)
        load_claude_prompt(token)
        print(f"[{job_id}] ✅ Prompts verified")

        # ── Fetch quote ───────────────────────────────────────
        _phase(context, job_id, "Fetching quote from Zoho...")
        quote = fetch_zoho_quote(quote_id, token)
        print(f"[{job_id}] Quote fields: {list(quote.keys())}")

        # ── Margin gate ───────────────────────────────────────
        _phase(context, job_id, "Checking vendor & opportunity margins...")
        margin = check_margin_gate(quote, token)
        if margin["blocked"]:
            print(f"[{job_id}] ⚠️  Margin gate NEEDS REVIEW — continuing anyway")

        # ── Format quote for Claude ───────────────────────────
        if _is_cancelled(context, job_id): return _resp(200, {"job_id": job_id})
        zoho_text   = format_zoho_quote(quote)
        quote_ref   = quote.get("Quotation_Reference", "")
        safe_ref    = re.sub(r'[^\w\-_.]', '_', str(quote_ref).strip())
        report_name = f"DOC_Compare_{safe_ref}.pdf"

        # ── Validate PDF attachments ──────────────────────────
        ppo_field = quote.get(PPO_PDF_FIELD)
        vq_field  = quote.get(VQ_PDF_FIELD)
        missing   = []
        if not ppo_field or not isinstance(ppo_field, list) or not ppo_field:
            missing.append(f"Partner PO PDF (field: {PPO_PDF_FIELD})")
        if not vq_field or not isinstance(vq_field, list) or not vq_field:
            missing.append(f"Vendor Quote PDF (field: {VQ_PDF_FIELD})")
        if missing:
            raise Exception(
                "Required PDF attachments are missing from this quote record. "
                "Please attach the following files before running comparison:\n"
                + "\n".join(f"  - {m}" for m in missing)
            )

        fid_ppo = ppo_field[0].get("file_Id")
        fid_vq  = vq_field[0].get("file_Id")
        if not fid_ppo:
            raise Exception(f"Partner PO PDF is attached but has no file ID. Re-attach {PPO_PDF_FIELD}.")
        if not fid_vq:
            raise Exception(f"Vendor Quote PDF is attached but has no file ID. Re-attach {VQ_PDF_FIELD}.")

        # ── Download PDFs in parallel ─────────────────────────
        _phase(context, job_id, "Downloading PDF attachments...")
        ppo_bytes, vq_bytes = _run_parallel(
            lambda: download_zoho_file(fid_ppo, token),
            lambda: download_zoho_file(fid_vq,  token),
        )

        if not is_valid_pdf(ppo_bytes):
            raise Exception(f"{PPO_PDF_FIELD} is not a valid PDF file.")
        if not is_valid_pdf(vq_bytes):
            raise Exception(f"{VQ_PDF_FIELD} is not a valid PDF file.")

        # ── Gemini extraction in parallel ─────────────────────
        _phase(context, job_id, "Extracting line items with Gemini AI...")
        gemini_model  = get_gemini_model()
        gemini_prompt = load_gemini_prompt(token)

        (ppo_text, ppo_header), (vq_text, vq_header) = _run_parallel(
            lambda: extract_pdf_gemini(ppo_bytes, "Partner PO PDF", gemini_model, gemini_prompt),
            lambda: extract_pdf_gemini(vq_bytes,  "VQ PDF",         gemini_model, gemini_prompt),
        )

        print(f"[{job_id}] VQ TEXT: {vq_text[:200]}")
        print(f"[{job_id}] PPO TEXT: {ppo_text[:200]}")

        # ── Claude comparison ─────────────────────────────────
        _phase(context, job_id, "Comparing documents with Claude AI...")
        if _is_cancelled(context, job_id): return _resp(200, {"job_id": job_id})
        result = run_comparison(zoho_text, ppo_text, vq_text, token)

        # ── Attach margin gate & subtotal validation ──────────
        result["margin_gate"] = {
            "checked":          margin["skipped_reason"] is None,
            "needs_review":     margin["blocked"],
            "opportunity_name": margin["opportunity_name"],
            "vendor_name":      margin["vendor_name"],
            "gross_margin":     margin["gross_margin"],
            "vendor_margin":    margin["vendor_margin"],
        }
        result["subtotal_validation"] = check_subtotal_validation(
            quote, margin, ppo_header, vq_header
        )

        # ── Generate PDF ──────────────────────────────────────
        _phase(context, job_id, "Generating PDF report...")
        pdf_bytes = generate_pdf_report(result, quote.get("Subject", quote_id), initiated_by)

        # ── Attach PDF to quote ───────────────────────────────
        _phase(context, job_id, "Attaching report to Zoho quote...")
        attach_pdf_to_quote(quote_id, pdf_bytes, token, report_name)

        # ── Done ──────────────────────────────────────────────
        generated_at = datetime.now().isoformat()
        _ds_write(context, job_id, {
            "status":       "done",
            "result_json":  json.dumps(result),
            "generated_at": generated_at,
            "quote_ref":    quote_ref
        })
        print(f"[{job_id}] ✅ Complete")
        return _resp(200, {"job_id": job_id})

    except Exception as e:
        import traceback
        print(f"[{job_id}] ❌ {traceback.format_exc()}")
        if _ds_read(context, job_id).get("status") != "cancelled":
            _ds_write(context, job_id, {"status": "error", "error": str(e)})
        return _resp(200, {"job_id": job_id})


def _handle_job_status(context, job_id: str):
    row = _ds_read(context, job_id)
    if not row:
        return _resp(200, {"status": "not_found"})

    status = row.get("status", "processing")
    if status == "done":
        try:
            result = json.loads(row.get("result_json", "{}"))
        except Exception:
            result = {}
        return _resp(200, {
            "status":       "done",
            "result":       result,
            "generated_at": row.get("generated_at"),
            "quote_ref":    row.get("quote_ref")
        })

    return _resp(200, {
        "status": status,
        "phase":  row.get("phase", "Processing..."),
        "error":  row.get("error")
    })


def _handle_check_report(context, quote_id: str):
    try:
        token      = _get_token(context)
        attachment = check_existing_report(quote_id, token)
        if attachment:
            return _resp(200, {
                "exists":        True,
                "attachment_id": attachment.get("id"),
                "file_name":     attachment.get("File_Name"),
                "created_time":  attachment.get("Created_Time")
            })
        return _resp(200, {"exists": False})
    except Exception as e:
        return _resp(200, {"exists": False, "error": str(e)})


def _handle_cancel(context, job_id: str):
    row = _ds_read(context, job_id)
    if row.get("status") == "processing":
        _ds_write(context, job_id, {"status": "cancelled"})
        print(f"[{job_id}] ❌ Cancelled by user")
    return _resp(200, {"cancelled": True})


# ═════════════════════════════════════════════════════════════
# PROMPT LOADER  (identical to app.py — same CRM AIPrompts module)
# ═════════════════════════════════════════════════════════════
def _sanitise_prompt(text: str) -> str:
    INVISIBLE = ("\ufeff","\u2060","\u200b","\u200c","\u200d","\u00a0","\u2028","\u2029")
    for ch in INVISIBLE:
        text = text.replace(ch, " " if ch == "\u00a0" else "")
    return text.strip()


def _fetch_prompts_from_crm(token: str) -> tuple:
    url     = f"{ZOHO_BASE_URL}/crm/v3/AIPrompts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params  = {"fields": "GEMINI_PROMPT,CLAUDE_PROMPT", "per_page": 1}

    r = requests.get(url, headers=headers, params=params, timeout=20)
    print(f"[prompts] AIPrompts fetch status: {r.status_code}")

    if r.status_code == 204:
        raise RuntimeError("AIPrompts module is empty.")
    if not r.ok:
        raise RuntimeError(f"AIPrompts fetch failed: {r.status_code} {r.text[:500]}")

    data = r.json().get("data", [])
    if not data:
        raise RuntimeError("AIPrompts module is empty.")

    record        = data[0]
    gemini_prompt = _sanitise_prompt(record.get("GEMINI_PROMPT") or "")
    claude_prompt = _sanitise_prompt(record.get("CLAUDE_PROMPT") or "")

    if not gemini_prompt:
        raise RuntimeError("GEMINI_PROMPT field in AIPrompts is blank.")
    if not claude_prompt:
        raise RuntimeError("CLAUDE_PROMPT field in AIPrompts is blank.")

    print(f"[prompts] Loaded — Gemini: {len(gemini_prompt)} chars, Claude: {len(claude_prompt)} chars")
    return gemini_prompt, claude_prompt


def _refresh_prompt_cache(token: str) -> tuple:
    now = time.time()
    try:
        gp, cp = _fetch_prompts_from_crm(token)
        _crm_prompt_cache["gemini"] = (gp, now)
        _crm_prompt_cache["claude"] = (cp, now)
        return gp, cp
    except Exception as e:
        print(f"[prompts] ⚠️  CRM fetch failed: {e}")
        gt, _ = _crm_prompt_cache["gemini"]
        ct, _ = _crm_prompt_cache["claude"]
        if gt and ct:
            print("[prompts] Using stale cached prompts")
            return gt, ct
        raise RuntimeError(
            f"Cannot load AI prompts — no cache available. Error: {e}"
        ) from e


def load_gemini_prompt(token: str = "") -> str:
    now = time.time()
    text, fetched_at = _crm_prompt_cache["gemini"]
    if text and (now - fetched_at) < _PROMPT_CACHE_TTL:
        return text
    return _refresh_prompt_cache(token)[0]


def load_claude_prompt(token: str = "") -> str:
    now = time.time()
    text, fetched_at = _crm_prompt_cache["claude"]
    if text and (now - fetched_at) < _PROMPT_CACHE_TTL:
        return text
    return _refresh_prompt_cache(token)[1]


# ═════════════════════════════════════════════════════════════
# ZOHO API HELPERS  (identical to app.py)
# ═════════════════════════════════════════════════════════════
def _raise_for_zoho(r):
    """
    r.raise_for_status() alone drops the response body — Zoho's OAuth/API errors
    (wrong scope, wrong data center, expired token, etc.) are JSON in the body,
    not the status line, so surface it instead of a bare '401 Client Error'.
    """
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.reason} for {r.url}: {r.text[:500]}", response=r)


def fetch_zoho_quote(quote_id: str, token: str) -> dict:
    url = f"{ZOHO_BASE_URL}/crm/v3/Quotes/{quote_id}"
    r   = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {token}"}, timeout=30)
    _raise_for_zoho(r)
    quote = r.json()["data"][0]
    print(f"✅ Quote fetched: {quote.get('Subject', quote_id)}")
    return quote


def download_zoho_file(file_id: str, token: str) -> bytes:
    url = f"{ZOHO_BASE_URL}/crm/v3/files?id={file_id}"
    r   = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {token}"}, timeout=60)
    _raise_for_zoho(r)
    print(f"✅ Downloaded {file_id} ({len(r.content)} bytes)")
    return r.content


def check_existing_report(quote_id: str, token: str, report_name: str = None):
    url = f"{ZOHO_BASE_URL}/crm/v3/Quotes/{quote_id}/Attachments"
    r   = requests.get(url,
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
            params={"fields": "id,File_Name,Created_Time,Size"},
            timeout=30)
    if r.status_code in (204, 404):
        return None
    _raise_for_zoho(r)
    for a in r.json().get("data", []):
        fname = a.get("File_Name", "")
        if report_name and fname == report_name:
            return a
        elif not report_name and fname.startswith("DOC_Compare_"):
            return a
    return None


def attach_pdf_to_quote(quote_id: str, pdf_bytes: bytes, token: str, report_name: str = "SKU_Audit_Report.pdf"):
    existing = check_existing_report(quote_id, token, report_name)
    if existing:
        del_url = f"{ZOHO_BASE_URL}/crm/v3/Quotes/{quote_id}/Attachments/{existing['id']}"
        requests.delete(del_url, headers={"Authorization": f"Zoho-oauthtoken {token}"}, timeout=30)

    url = f"{ZOHO_BASE_URL}/crm/v3/Quotes/{quote_id}/Attachments"
    r   = requests.post(url,
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
            files={"file": (report_name, pdf_bytes, "application/pdf")},
            timeout=60)
    if r.status_code == 400:
        return _attach_via_filestore(quote_id, pdf_bytes, token, report_name)
    _raise_for_zoho(r)
    return r.json().get("data", [{}])[0].get("details", {}).get("id")


def _attach_via_filestore(quote_id: str, pdf_bytes: bytes, token: str, report_name: str):
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    r = requests.post(f"{ZOHO_BASE_URL}/crm/v3/files",
            headers=headers,
            files={"file": (report_name, pdf_bytes, "application/pdf")},
            timeout=60)
    _raise_for_zoho(r)
    file_id = r.json().get("data", [{}])[0].get("details", {}).get("id")
    if not file_id:
        raise Exception("No file_id from filestore: " + r.text[:200])
    r2 = requests.post(f"{ZOHO_BASE_URL}/crm/v3/Quotes/{quote_id}/Attachments",
            headers={**headers, "Content-Type": "application/json"},
            json={"attachments": [{"id": file_id}]},
            timeout=30)
    _raise_for_zoho(r2)
    return r2.json().get("data", [{}])[0].get("details", {}).get("id")


def is_valid_pdf(b: bytes) -> bool:
    return b[:5] == b"%PDF-"


# ═════════════════════════════════════════════════════════════
# FORMAT ZOHO QUOTE  (identical to app.py)
# ═════════════════════════════════════════════════════════════
def format_zoho_quote(quote: dict) -> str:
    lines = ["## QUOTE HEADER"]
    for field, label in [
        ("Currency",             "Quote_Currency"),
        ("Partner_PO_Currency",  "Partner_PO_Currency"),
        ("Vendor_Quote_Currency","Vendor_Quote_Currency"),
        ("Exchange_Rate",        "Exchange_Rate"),
        ("Reseller",             "Reseller"),
        ("Partner_PO_Ref",       "Partner_PO_Ref"),
        ("Vendor",               "Vendor"),
        ("Vendor_Quote_Ref",     "Vendor_Quote_Ref"),
    ]:
        lines.append(f"  {label:24}: {quote.get(field, 'N/A')}")
    lines.append("")
    items = quote.get("Quoted_Items", [])
    print(f"✅ Zoho quote has {len(items)} line items")
    lines.append("## LINE ITEMS")
    for i, item in enumerate(items, 1):
        pn       = item.get("Product_Name") or {}
        sku      = pn.get("Product_Code") or pn.get("name") or "N/A"
        raw_buy  = item.get("Buy_Price")
        raw_list = item.get("List_Price")
        buy      = round(raw_buy,  2) if isinstance(raw_buy,  (int, float)) else (raw_buy  or "N/A")
        lst      = round(raw_list, 2) if isinstance(raw_list, (int, float)) else (raw_list or "N/A")
        lines += [
            f"  {i}. SKU          : {sku}",
            f"     Description  : {item.get('Description', 'N/A')}",
            f"     Quantity     : {item.get('Quantity', 'N/A')}",
            f"     buy_price_zq : {buy}",
            f"     list_price_zq: {lst}",
            ""
        ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════
# GEMINI  (identical to app.py)
# ═════════════════════════════════════════════════════════════
def get_gemini_model() -> str:
    global _gemini_model_cache
    if _gemini_model_cache:
        return _gemini_model_cache
    genai.configure(api_key=GEMINI_API_KEY)
    try:
        models = [m.name for m in genai.list_models(request_options={"timeout": 10})
                  if "generateContent" in m.supported_generation_methods]
        for m in models:
            if "gemini-1.5-flash" in m:
                _gemini_model_cache = m
                return m
        _gemini_model_cache = models[0]
        return _gemini_model_cache
    except Exception as e:
        print(f"⚠️  list_models failed ({e}), using gemini-1.5-flash")
        _gemini_model_cache = "models/gemini-1.5-flash"
        return _gemini_model_cache


def extract_pdf_gemini(pdf_bytes: bytes, label: str, model_name: str, prompt: str = "") -> tuple:
    model = genai.GenerativeModel(
        model_name=model_name,
        generation_config={"temperature": 0, "response_mime_type": "application/json"}
    )
    print(f"🔍 Extracting {label} via Gemini ({model_name})...")
    response = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, prompt],
        request_options={"timeout": 120}
    )
    raw = response.text
    print(f"✅ Gemini {label}: {len(raw)} chars")

    try:
        clean  = raw.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, dict) and "line_items" in parsed:
            header = parsed.get("header") or {}
            items  = parsed.get("line_items") or []
        elif isinstance(parsed, list):
            header, items = {}, parsed
        else:
            raise ValueError(f"Unexpected Gemini response: {type(parsed)}")

        print(f"✅ Extracted {len(items)} items from {label}, header: {header}")
        lines = [f"## {label}", "### HEADER FIELDS"]
        for k in ("reseller_name","partner_po_ref","vendor_name","vendor_quote_ref",
                  "partner_po_subtotal","vendor_quote_subtotal"):
            val = header.get(k)
            lines.append(f"  {k:25}: {val if val is not None else 'null'}")
        lines.append("")
        lines.append("### LINE ITEMS")
        for it in items:
            lines.append(
                f"  {it.get('line_num','')}. SKU: {it.get('sku','N/A')} | "
                f"Desc: {it.get('description','N/A')} | "
                f"Qty: {it.get('quantity','N/A')} | "
                f"list_unit_price: {it.get('list_unit_price','N/A')}"
            )
        return "\n".join(lines), header

    except (json.JSONDecodeError, ValueError) as e:
        print(f"⚠️  Gemini parse error for {label}: {e}")
        return raw, {}


# ═════════════════════════════════════════════════════════════
# CLAUDE COMPARISON  (identical to app.py)
# ═════════════════════════════════════════════════════════════
def run_comparison(zoho_text: str, ppo_text: str, vq_text: str, token: str = "") -> dict:
    matching_prompt = load_claude_prompt(token)
    print(f"[claude] Prompt: {len(matching_prompt)} chars")
    client    = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    full_text = ""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=32000,
        temperature=0,
        messages=[{"role": "user", "content": [
            {"type": "text", "text": "## ZOHO QUOTE (ZQ):\n\n" + zoho_text + "\n\n---"},
            {"type": "text", "text": "## VENDOR QUOTE (VQ) JSON:\n\n" + vq_text + "\n\n---"},
            {"type": "text", "text": "## Partner PO (PO):\n\n" + ppo_text + "\n\n---"},
            {"type": "text", "text": matching_prompt}
        ]}]
    ) as stream:
        for chunk in stream.text_stream:
            full_text += chunk
        msg = stream.get_final_message()
        if msg.stop_reason == "max_tokens":
            raise Exception("Claude truncated — increase max_tokens")

    print(f"[claude] Response: {len(full_text)} chars | stop: {msg.stop_reason}")
    clean = full_text.replace("```json", "").replace("```", "").strip()
    match = re.search(r'\{.*\}', clean, re.DOTALL)
    if not match:
        raise Exception(f"No JSON in Claude response. raw={repr(full_text[:300])}")
    return json.loads(match.group())


# ═════════════════════════════════════════════════════════════
# MARGIN GATE  (identical to app.py)
# ═════════════════════════════════════════════════════════════
def parse_percentage(value):
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    s = str(value).strip().replace("%", "")
    try: return float(s)
    except ValueError: return None


def parse_amount(value):
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    s = re.sub(r"[,$€£]", "", str(value).strip())
    s = re.sub(r"\b(USD|AED|SAR|QAR)\b", "", s, flags=re.IGNORECASE).strip()
    try: return float(s)
    except ValueError: return None


def convert_to_usd(amount, currency):
    if amount is None or not currency: return None
    rate = _USD_RATES.get(str(currency).strip().upper())
    return round(amount * rate, 2) if rate else None


def search_zoho_record_by_name(module, name, name_field, token):
    if not name: return None
    url = f"{ZOHO_BASE_URL}/crm/v3/{module}/search"
    r   = requests.get(url,
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
            params={"criteria": f"({name_field}:equals:{name})"},
            timeout=30)
    if r.status_code == 204: return None
    _raise_for_zoho(r)
    data = r.json().get("data", [])
    return data[0] if data else None


def fetch_zoho_record(module, record_id, token, fields=None):
    url = f"{ZOHO_BASE_URL}/crm/v3/{module}/{record_id}"
    r   = requests.get(url,
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
            params={"fields": fields} if fields else {},
            timeout=30)
    _raise_for_zoho(r)
    data = r.json().get("data", [])
    if not data: raise Exception(f"No record found: {module}/{record_id}")
    return data[0]


def resolve_margin_by_name(raw_value, module, name_field, margin_field, token, extra_fields=None):
    if not raw_value or raw_value == "N/A": return None, None, None
    fields_to_request = margin_field if not extra_fields else f"{margin_field},{extra_fields}"
    if isinstance(raw_value, dict):
        record_id    = raw_value.get("id")
        display_name = raw_value.get("name")
        if record_id:
            record = fetch_zoho_record(module, record_id, token, fields=fields_to_request)
            return display_name, parse_percentage(record.get(margin_field)), record
        raw_value = display_name
    if not raw_value: return None, None, None
    found = search_zoho_record_by_name(module, raw_value, name_field, token)
    if not found:
        print(f"⚠️  No {module} found for '{raw_value}'")
        return raw_value, None, None
    record = fetch_zoho_record(module, found["id"], token, fields=f"{fields_to_request},{name_field}")
    return record.get(name_field) or raw_value, parse_percentage(record.get(margin_field)), record


def check_margin_gate(quote: dict, token: str) -> dict:
    outcome = {
        "blocked": False, "opportunity_name": None, "vendor_name": None,
        "gross_margin": None, "vendor_margin": None, "skipped_reason": None,
        "amount_in_usd": None, "net_to_vendor": None,
    }
    try:
        vendor_raw      = quote.get("Vendor")
        opportunity_raw = quote.get(OPPORTUNITY_FIELD_ON_QUOTE)
        vendor_display, vendor_margin, _ = resolve_margin_by_name(
            vendor_raw, VENDORS_MODULE, VENDORS_NAME_FIELD, VENDOR_MARGIN_FIELD, token
        )
        opp_display, gross_margin, opp_record = resolve_margin_by_name(
            opportunity_raw, OPPORTUNITIES_MODULE, OPPORTUNITIES_NAME_FIELD, GROSS_MARGIN_FIELD, token,
            extra_fields=f"{AMOUNT_IN_USD_FIELD},{NET_TO_VENDOR_FIELD}"
        )
    except Exception as e:
        print(f"⚠️  Margin gate error — skipping: {e}")
        outcome["skipped_reason"] = f"Margin gate error: {e}"
        return outcome

    outcome.update({"opportunity_name": opp_display, "vendor_name": vendor_display,
                    "gross_margin": gross_margin, "vendor_margin": vendor_margin})
    if opp_record:
        outcome["amount_in_usd"] = parse_amount(opp_record.get(AMOUNT_IN_USD_FIELD))
        outcome["net_to_vendor"] = parse_amount(opp_record.get(NET_TO_VENDOR_FIELD))

    if gross_margin is None or vendor_margin is None:
        missing = []
        if gross_margin  is None: missing.append(f"Gross Margin (Opportunity: {opp_display or 'not set'})")
        if vendor_margin is None: missing.append(f"Vendor Margin (Vendor: {vendor_display or 'not set'})")
        outcome["skipped_reason"] = "Missing: " + ", ".join(missing)
        return outcome

    print(f"📊 Margin — Gross: {gross_margin}% | Vendor: {vendor_margin}%")
    if gross_margin < vendor_margin:
        outcome["blocked"] = True
    return outcome


def check_subtotal_validation(quote, margin, ppo_header, vq_header) -> dict:
    ppc = quote.get("Partner_PO_Currency") or ""
    vqc = quote.get("Vendor_Quote_Currency") or ""
    ppo_raw = parse_amount((ppo_header or {}).get("partner_po_subtotal"))
    vq_raw  = parse_amount((vq_header  or {}).get("vendor_quote_subtotal"))
    ppo_usd = convert_to_usd(ppo_raw, ppc)
    vq_usd  = convert_to_usd(vq_raw,  vqc)
    amt_usd = margin.get("amount_in_usd")
    net_vnd = margin.get("net_to_vendor")

    def _cmp(pdf_usd, opp_val):
        if pdf_usd is None or opp_val is None: return "Needs Review"
        return "Match" if abs(pdf_usd - opp_val) <= SUBTOTAL_TOLERANCE_USD else "Mismatch"

    ps = _cmp(ppo_usd, amt_usd)
    vs = _cmp(vq_usd,  net_vnd)
    overall = "Mismatch" if "Mismatch" in (ps, vs) else ("Needs Review" if "Needs Review" in (ps, vs) else "Match")
    print(f"💰 Subtotal — PPO: {ppo_usd} vs {amt_usd} → {ps} | VQ: {vq_usd} vs {net_vnd} → {vs}")
    return {
        "overall_status": overall,
        "partner_po":     {"label":"Partner PO Subtotal vs Amount (USD)","pdf_subtotal":ppo_raw,"pdf_currency":ppc or None,"pdf_subtotal_usd":ppo_usd,"opportunity_value":amt_usd,"status":ps},
        "vendor_quote":   {"label":"Vendor Quote Subtotal vs Net to Vendor (USD)","pdf_subtotal":vq_raw,"pdf_currency":vqc or None,"pdf_subtotal_usd":vq_usd,"opportunity_value":net_vnd,"status":vs},
    }


# ═════════════════════════════════════════════════════════════
# GENERATE PDF REPORT  (identical to app.py — full HTML template)
# ═════════════════════════════════════════════════════════════
def generate_pdf_report(result: dict, quote_subject: str, initiated_by: str = "") -> bytes:
    print("Generating PDF report...")
    t0 = time.time()

    def status_badge(status):
        if not status or status == "-": return '<span class="pill pill-na">N/A</span>'
        s = status.lower()
        if "mismatch"  in s: return '<span class="pill pill-miss">Mismatch</span>'
        if "not found" in s: return '<span class="pill pill-review">Not Found</span>'
        if "review"    in s: return '<span class="pill pill-review">Review</span>'
        if "match"     in s: return '<span class="pill pill-match">Match</span>'
        return f'<span class="pill pill-na">{status}</span>'

    fc = (result.get("final_call") or "").upper()
    if "CLEAR" in fc:   banner_bg, banner_border = "#d1fae5", "#10b981"
    elif "HOLD" in fc:  banner_bg, banner_border = "#fee2e2", "#ef4444"
    else:               banner_bg, banner_border = "#fef3c7", "#f59e0b"

    fc_details = "".join([f"<li>{d}</li>" for d in (result.get("final_call_detail") or [])])

    # Margin gate banner
    mg = result.get("margin_gate") or {}
    margin_status_block = ""
    if mg.get("checked"):
        gm, vm = mg.get("gross_margin"), mg.get("vendor_margin")
        if mg.get("needs_review"):
            margin_status_block = f"""<div class="margin-needs-review-banner">
            <div class="mnr-title">&#9888; Margin Check — Needs Review</div>
            <p class="mnr-detail">Gross Margin ({gm if gm is not None else "—"}%) is less than
            Minimum Vendor Margin ({vm if vm is not None else "—"}%) — flagged for review.</p>
            <div class="margin-stats-pdf">
              <div class="ms-item"><span class="ms-label">Opportunity</span><span class="ms-value">{mg.get("opportunity_name") or "—"}</span></div>
              <div class="ms-item"><span class="ms-label">Vendor</span><span class="ms-value">{mg.get("vendor_name") or "—"}</span></div>
              <div class="ms-item"><span class="ms-label">Gross Margin</span><span class="ms-value" style="color:#b91c1c">{gm if gm is not None else "—"}%</span></div>
              <div class="ms-item"><span class="ms-label">Min Vendor Margin</span><span class="ms-value" style="color:#065f46">{vm if vm is not None else "—"}%</span></div>
            </div></div>"""
        else:
            margin_status_block = f"""<div class="margin-pass-banner">
            <span class="mp-title">&#10003; Margin check passed</span>
            <span class="mp-stat"><span class="mp-label">Gross Margin</span>{gm if gm is not None else "—"}%</span>
            <span class="mp-stat"><span class="mp-label">Min Vendor Margin</span>{vm if vm is not None else "—"}%</span>
            </div>"""

    # Currency block
    currencies = result.get("currencies_detected") or {}
    qc  = currencies.get("quote_currency") or "—"
    ppc = currencies.get("partner_po_currency") or "—"
    vqc = currencies.get("vendor_quote_currency") or "—"
    cur_notes = currencies.get("notes") or ""
    currency_block = f"""<div class="card currency-card">
        <div class="card-title">Currency Overview</div>
        <div class="currency-row">
          <div class="currency-item"><span class="currency-tag-label">Zoho Quote</span><span class="currency-tag-value">{qc}</span></div>
          <div class="currency-item"><span class="currency-tag-label">Partner PO</span><span class="currency-tag-value">{ppc}</span></div>
          <div class="currency-item"><span class="currency-tag-label">Vendor Quote</span><span class="currency-tag-value">{vqc}</span></div>
        </div>
        {f'<p class="currency-notes">{cur_notes}</p>' if cur_notes else ""}
      </div>"""

    # Document header validation block
    dhv = result.get("document_header_validation") or {}
    def _dhv_row(label, fd):
        if not fd: return ""
        return (f"<tr><td style='font-weight:600;font-size:9px'>{label}</td>"
                f"<td style='font-size:9px;font-family:monospace'>{fd.get('zq_value') or '—'}</td>"
                f"<td style='font-size:9px;font-family:monospace'>{fd.get('pdf_value') or '—'}</td>"
                f"<td style='text-align:center'>{status_badge(fd.get('status'))}</td>"
                f"<td style='font-size:9px;color:#6b7280'>{fd.get('note') or ''}</td></tr>")
    dhv_rows = (_dhv_row("Reseller", dhv.get("reseller"))
              + _dhv_row("Partner PO Ref", dhv.get("partner_po_ref"))
              + _dhv_row("Vendor", dhv.get("vendor"))
              + _dhv_row("Vendor Quote Ref", dhv.get("vendor_quote_ref")))
    header_validation_block = ""
    if dhv_rows:
        header_validation_block = f"""<div class="card">
        <div class="card-title">Document Header Validation</div>
        <table><thead><tr>
          <th style="width:110px">Field</th>
          <th style="width:170px">Zoho Quote Value</th>
          <th style="width:170px">PDF Extracted Value</th>
          <th style="width:82px;text-align:center">Status</th>
          <th>Note</th>
        </tr></thead><tbody>{dhv_rows}</tbody></table></div>"""

    # SKU blocks
    def sku_block(r, i):
        row_bg = "#ffffff" if i % 2 == 0 else "#f9fafb"
        all_s  = [r.get(f,"") for f in ("zq_status","vq_status","ppo_status","list_price_comparison_status","buy_price_comparison_status")]
        worst  = "match"
        for s in all_s:
            sl = (s or "").lower()
            if "mismatch" in sl: worst = "mismatch"; break
            if "review"   in sl and worst != "mismatch": worst = "review"
        if worst == "mismatch": pb,pc,pl = "#fee2e2","#991b1b","Mismatch"
        elif worst == "review": pb,pc,pl = "#fef3c7","#92400e","Review"
        else:                   pb,pc,pl = "#d1fae5","#065f46","Match"
        overall_pill = f'<span class="pill" style="background:{pb};color:{pc}">{pl}</span>'
        zqq  = str(r.get("zq_qty"))  if r.get("zq_qty")  is not None else "-"
        vqq  = str(r.get("vq_qty"))  if r.get("vq_qty")  is not None else "-"
        ppoq = str(r.get("ppo_qty")) if r.get("ppo_qty") is not None else "-"
        all_qty_match = all(
            ("match" in (r.get(f) or "").lower() and "mismatch" not in (r.get(f) or "").lower())
            for f in ["zq_status","vq_status","ppo_status"]
        )
        if all_qty_match:
            qty_row = f"""<tr><td class="rl">Qty</td>
              <td class="cv">{zqq}</td><td class="cv">{ppoq}</td><td class="cv">{vqq}</td>
              <td class="sc">{status_badge("Match")}</td></tr>"""
        else:
            qty_row = f"""<tr><td class="rl">Qty</td>
              <td class="cv">{zqq}</td><td class="cv">{ppoq}</td><td class="cv">{vqq}</td>
              <td class="sc">
                <div style="font-size:7px;color:#6b7280">ZQ&#8596;PPO {status_badge(r.get("zq_status"))}</div>
                <div style="font-size:7px;color:#6b7280">ZQ&#8596;VQ&nbsp;&nbsp;{status_badge(r.get("vq_status"))}</div>
                <div style="font-size:7px;color:#6b7280">PPO&#8596;VQ {status_badge(r.get("ppo_status"))}</div>
              </td></tr>"""
        note = (r.get("notes") or "").strip()
        note_row = f"""<tr><td class="rl">Notes</td>
          <td colspan="4" style="font-size:8px;color:#6b7280;line-height:1.4">{note}</td></tr>""" if note else ""
        return f"""<div class="sku-block" style="background:{row_bg}">
          <div class="sku-hdr">
            <span class="sku-num">{r.get("num") or i+1}</span>
            <span class="sku-code">{r.get("sku") or "—"}</span>
            <span style="margin-left:auto">{overall_pill}</span>
          </div>
          <table class="dt"><thead><tr>
            <th style="width:65px"></th><th>ZQ</th><th>PPO</th><th>VQ</th>
            <th style="width:110px;text-align:center">Status</th>
          </tr></thead><tbody>
            {qty_row}
            <tr style="background:#f8f8f8">
              <td class="rl">Price</td>
              <td class="cv" style="font-size:7px;color:#9ca3af;font-weight:600">ZQ</td>
              <td class="cv" style="font-size:7px;color:#9ca3af;font-weight:600">PPO</td>
              <td class="cv" style="font-size:7px;color:#9ca3af;font-weight:600">VQ</td>
              <td></td>
            </tr>
            <tr style="background:#f8f8f8">
              <td class="rl sub">List&nbsp;Price</td>
              <td class="cv">{r.get("list_price_zq") or "-"}</td>
              <td class="cv">{r.get("partner_ppo_price_original") or "-"}</td>
              <td class="cv" style="color:#d1d5db">—</td>
              <td class="sc">{status_badge(r.get("list_price_comparison_status"))}</td>
            </tr>
            <tr style="background:#f8f8f8">
              <td class="rl sub">Buy&nbsp;Price</td>
              <td class="cv">{r.get("buy_price_zq") or "-"}</td>
              <td class="cv" style="color:#d1d5db">—</td>
              <td class="cv">{r.get("vendor_quote_price") or "-"}</td>
              <td class="sc">{status_badge(r.get("buy_price_comparison_status"))}</td>
            </tr>
            {note_row}
          </tbody></table></div>"""

    sku_blocks = "".join(sku_block(r, i) for i, r in enumerate(result.get("matching_table", [])))

    # Subtotal validation block
    sv = result.get("subtotal_validation") or {}
    subtotal_validation_block = ""
    if sv:
        def _sv_row(d):
            if not d: return ""
            pu  = d.get("pdf_subtotal_usd")
            ov  = d.get("opportunity_value")
            pr  = d.get("pdf_subtotal")
            pus = f"${pu:,.2f}" if pu is not None else "—"
            ovs = f"${ov:,.2f}" if ov is not None else "—"
            prs = f"{pr:,.2f} {d.get('pdf_currency') or ''}".strip() if pr is not None else "—"
            return (f"<tr>"
                    f"<td style='font-weight:600;font-size:9px'>{d.get('label','')}</td>"
                    f"<td style='font-size:9px;font-family:monospace'>{prs}</td>"
                    f"<td style='font-size:9px;font-family:monospace'>{pus}</td>"
                    f"<td style='font-size:9px;font-family:monospace'>{ovs}</td>"
                    f"<td style='text-align:center'>{status_badge(d.get('status'))}</td></tr>")
        sv_rows = _sv_row(sv.get("partner_po")) + _sv_row(sv.get("vendor_quote"))
        subtotal_validation_block = f"""<div class="card">
        <div class="card-title">Subtotal Validation (vs Opportunity)</div>
        <table><thead><tr>
          <th style="width:220px">Check</th>
          <th style="width:140px">PDF Subtotal</th>
          <th style="width:100px">Converted (USD)</th>
          <th style="width:120px">Opportunity Value</th>
          <th style="width:82px;text-align:center">Status</th>
        </tr></thead><tbody>{sv_rows}</tbody></table>
        <p style="font-size:9px;color:#374151;margin-top:6px">Overall: {status_badge(sv.get("overall_status"))}</p>
      </div>"""

    must_resolve = "".join([f'<li class="item-red">{i}</li>'   for i in (result.get("must_resolve")    or [])]) or '<li style="color:#6b7280;font-style:italic">None — all items cleared</li>'
    needs_review = "".join([f'<li class="item-amber">{i}</li>' for i in (result.get("needs_review")    or [])]) or '<li style="color:#6b7280;font-style:italic">None — no items flagged for review</li>'
    unmatched    = "".join([f'<span class="tag">{i}</span>'    for i in (result.get("unmatched_items") or [])])
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    by_line      = f" &nbsp;|&nbsp; Initiated by: {initiated_by}" if initiated_by else ""

    html_content = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/>
<style>
  @page {{ size: A4 landscape; margin: 12mm; }}
  @page {{ -pdf-page-size: A4 landscape; }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: Arial, Helvetica, sans-serif; font-size: 11px; color: #1a1a2e; background: #f4f6f9; }}
  .header {{ margin-bottom: 12px; }}
  .header h1 {{ font-size: 18px; font-weight: bold; color: #1a1a2e; margin-bottom: 2px; }}
  .header .subtitle {{ font-size: 9px; color: #6b7280; }}
  .banner {{ border-radius: 6px; padding: 9px 12px; margin-bottom: 12px; border-left: 5px solid {banner_border}; background: {banner_bg}; }}
  .banner-title {{ font-weight: bold; font-size: 12px; color: #1a1a2e; margin-bottom: 3px; }}
  .banner ul {{ list-style: none; padding: 0; margin: 0; }}
  .banner ul li {{ font-size: 9px; color: #374151; padding: 1px 0; line-height: 1.5; }}
  .banner ul li:before {{ content: "- "; }}
  .card {{ background: #fff; border-radius: 6px; padding: 10px 12px; margin-bottom: 12px; border: 1px solid #e5e7eb; }}
  .card-title {{ font-size: 9px; font-weight: bold; text-transform: uppercase; letter-spacing: 0.06em; color: #374151; border-bottom: 2px solid #f3f4f6; padding-bottom: 5px; margin-bottom: 8px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 9px; }}
  thead th {{ background: #1a1a2e; color: #fff; padding: 6px 7px; text-align: left; font-weight: 600; font-size: 8px; text-transform: uppercase; letter-spacing: 0.04em; }}
  tbody td {{ padding: 5px 7px; border-bottom: 1px solid #f3f4f6; vertical-align: top; line-height: 1.4; }}
  .pill {{ display: inline-block; padding: 1px 6px; border-radius: 8px; font-size: 8px; font-weight: 600; }}
  .pill-match  {{ background: #d1fae5; color: #065f46; }}
  .pill-review {{ background: #fef3c7; color: #92400e; }}
  .pill-miss   {{ background: #fee2e2; color: #991b1b; }}
  .pill-na     {{ background: #f3f4f6; color: #9ca3af; }}
  .sku-block  {{ border: 1px solid #e5e7eb; border-radius: 5px; margin-bottom: 6px; overflow: hidden; }}
  .sku-hdr    {{ display: flex; align-items: center; gap: 8px; padding: 5px 8px; background: #1a1a2e; color: #fff; }}
  .sku-num    {{ font-size: 8px; color: #9ca3af; min-width: 14px; }}
  .sku-code   {{ font-family: monospace; font-size: 9px; font-weight: 700; color: #fff; }}
  .dt         {{ width: 100%; border-collapse: collapse; font-size: 8px; }}
  .dt thead th {{ background: #374151; color: #fff; padding: 4px 6px; text-align: left; font-size: 7px; font-weight: 600; text-transform: uppercase; }}
  .dt tbody td {{ padding: 4px 6px; border-bottom: 1px solid #f3f4f6; vertical-align: middle; }}
  .dt .rl     {{ font-weight: 700; color: #374151; font-size: 8px; white-space: nowrap; }}
  .dt .rl.sub {{ font-weight: 400; color: #6b7280; padding-left: 14px; font-size: 7px; }}
  .dt .cv     {{ font-family: monospace; font-size: 8px; color: #1a1a2e; }}
  .dt .sc     {{ text-align: left; }}
  .summary-label {{ font-weight: bold; font-size: 9px; margin: 7px 0 3px 0; }}
  .label-red   {{ color: #ef4444; }}
  .label-amber {{ color: #f59e0b; }}
  .label-green {{ color: #10b981; }}
  ul.summary-list {{ list-style: none; padding: 0; margin: 0 0 5px 0; }}
  ul.summary-list li {{ font-size: 9px; color: #374151; line-height: 1.5; padding: 2px 0 2px 8px; margin-bottom: 2px; }}
  li.item-red   {{ border-left: 3px solid #ef4444; }}
  li.item-amber {{ border-left: 3px solid #f59e0b; }}
  .tag {{ display: inline-block; background: #fee2e2; color: #991b1b; border-radius: 3px; padding: 1px 5px; font-size: 8px; margin: 2px; font-family: monospace; }}
  .overall-text {{ font-size: 9px; color: #374151; line-height: 1.6; }}
  .currency-card {{ padding-bottom: 10px; }}
  .currency-row {{ display: flex; gap: 12px; margin-bottom: 6px; }}
  .currency-item {{ background: #f4f6f9; border-radius: 5px; padding: 5px 10px; display: flex; flex-direction: column; gap: 1px; }}
  .currency-tag-label {{ font-size: 7px; text-transform: uppercase; letter-spacing: 0.05em; color: #9ca3af; font-weight: 700; }}
  .currency-tag-value {{ font-size: 11px; font-weight: 700; color: #1a1a2e; font-family: monospace; }}
  .currency-notes {{ font-size: 9px; color: #374151; line-height: 1.5; border-top: 1px solid #f3f4f6; padding-top: 6px; margin-top: 4px; }}
  .margin-pass-banner {{ background: #d1fae5; border: 1px solid #6ee7b7; border-radius: 6px; padding: 7px 12px; margin-bottom: 12px; font-size: 9px; color: #065f46; display: flex; align-items: center; gap: 16px; }}
  .margin-pass-banner .mp-title {{ font-weight: bold; }}
  .margin-pass-banner .mp-stat {{ font-family: monospace; font-weight: 600; }}
  .margin-pass-banner .mp-label {{ font-family: Arial, Helvetica, sans-serif; font-weight: normal; color: #047857; margin-right: 3px; }}
  .margin-needs-review-banner {{ background: #fef3c7; border: 1px solid #fcd34d; border-radius: 6px; padding: 8px 12px; margin-bottom: 12px; color: #92400e; }}
  .margin-needs-review-banner .mnr-title {{ font-weight: bold; font-size: 10px; margin-bottom: 3px; }}
  .margin-needs-review-banner .mnr-detail {{ font-size: 9px; color: #78350f; line-height: 1.5; margin-bottom: 6px; }}
  .margin-stats-pdf {{ display: flex; gap: 10px; flex-wrap: wrap; }}
  .margin-stats-pdf .ms-item {{ background: rgba(255,255,255,0.6); border-radius: 5px; padding: 4px 9px; display: flex; flex-direction: column; gap: 1px; min-width: 90px; }}
  .margin-stats-pdf .ms-label {{ font-size: 7px; text-transform: uppercase; letter-spacing: 0.05em; color: #78350f; font-weight: 700; }}
  .margin-stats-pdf .ms-value {{ font-size: 10px; font-weight: 700; color: #1a1a2e; font-family: monospace; }}
</style>
</head>
<body>
  <div class="header">
    <h1>Procurement Analysis Report</h1>
    <div class="subtitle">Quote: {quote_subject} &nbsp;|&nbsp; Generated: {generated_at}{by_line}</div>
  </div>
  {margin_status_block}
  {currency_block}
  {header_validation_block}
  <div class="banner">
    <div class="banner-title">{result.get("final_call","")}</div>
    <ul>{fc_details}</ul>
  </div>
  <div class="card">
    <div class="card-title">Section 1 - Three-Way Item Matching</div>
    {sku_blocks}
  </div>
  {subtotal_validation_block}
  <div class="card">
    <div class="card-title">Section 2 - Summary</div>
    <div class="summary-label label-red">Must Resolve Before Processing</div>
    <ul class="summary-list">{must_resolve}</ul>
    <div class="summary-label label-amber">Needs Human Review</div>
    <ul class="summary-list">{needs_review}</ul>
    {"<div class='summary-label label-red'>Unmatched Items</div><div>" + unmatched + "</div>" if unmatched else ""}
    <div class="summary-label label-green">Overall</div>
    <p class="overall-text">{result.get("overall_summary","")}</p>
  </div>
</body></html>"""

    buf = BytesIO()
    pisa_status = pisa.CreatePDF(html_content, dest=buf)
    if pisa_status.err:
        raise Exception(f"xhtml2pdf error: {pisa_status.err}")
    pdf_bytes = buf.getvalue()
    print(f"PDF generated: {len(pdf_bytes)} bytes in {time.time()-t0:.1f}s")
    return pdf_bytes
