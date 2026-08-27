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
  generated_at  — Datetime      (format "YYYY-MM-DD HH:MM:SS" — NOT isoformat(),
                                 confirmed live via INVALID_INPUT on write)
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

import requests
import zcatalyst_sdk
from flask import make_response, jsonify
from io import BytesIO

# anthropic, google.generativeai, and reportlab are deliberately NOT imported
# here — they're heavy (google.generativeai alone measured 2.7s+ to import,
# before even reaching its own native crypto deps). Importing them at module
# level meant EVERY cold start paid that cost even for /job-status or
# /check-report, which never touch Gemini/Claude/PDF at all. Imported lazily
# instead, inside the functions that actually use them.
#
# PDF generation used to go through xhtml2pdf (an HTML/CSS-to-PDF translation
# layer) — replaced with direct ReportLab Platypus calls in
# generate_pdf_report(); see that function's own header comment for why.


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
    """
    Write current phase to DataStore so the widget poll loop sees it.

    Skips the write if the job is already cancelled. The unconditional
    status="processing" write here used to silently clobber a "cancelled"
    status a concurrent /cancel-job request had just set — the moment the
    background pipeline thread reached its next _phase() call after a cancel
    landed, it stomped the status back to "processing", permanently erasing
    the signal for every downstream _is_cancelled() checkpoint. Confirmed
    live: this was why cancelling during an early phase still let the job
    run to completion, on both a fully redeployed widget and backend.
    """
    print(f"[{job_id}] {phase}")
    if _is_cancelled(context, job_id):
        return
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
    """
    Creates the job and returns job_id immediately — the widget expects this
    response fast so it can start polling /job-status for live progress (the
    original Cloud Run shape, via FastAPI BackgroundTasks). The real pipeline
    runs in a background thread (_run_analysis_pipeline) so this HTTP
    request/response cycle isn't held open for the full ~30s-540s duration.
    Running the whole pipeline synchronously in this one request (as before)
    held the widget's own HTTP client open past its client-side timeout, and
    starved /cancel-job and /job-status of a free worker to even execute on
    while /analyze-quote was still in flight — confirmed live: cancel silently
    did nothing, and the job always ran to completion regardless.
    """
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

    threading.Thread(
        target=_run_analysis_pipeline,
        args=(context, job_id, quote_id, initiated_by),
        daemon=False,
    ).start()

    return _resp(200, {"job_id": job_id})


def _run_analysis_pipeline(context, job_id: str, quote_id: str, initiated_by: str):
    """
    The actual compare pipeline — runs in the background thread _handle_analyze
    spawns. There is no HTTP response to return through from here: all progress
    and the final result/error go through DataStore (_phase/_ds_write) for the
    widget's /job-status polling to pick up.
    """
    try:
        token = _get_token(context)

        # ── Validate prompts first (fail fast) ───────────────
        load_gemini_prompt(token)
        load_claude_prompt(token)
        print(f"[{job_id}] ✅ Prompts verified")

        # ── Fetch quote ───────────────────────────────────────
        _phase(context, job_id, "Fetching quote from Zoho...")
        # Cosmetic delay only — this single REST call is fast enough (Catalyst
        # calls zohoapis.com intra-network vs. Cloud Run's cross-cloud path)
        # that the widget's 3s poll can otherwise skip straight past this
        # phase. Padding so it's reliably visible; doesn't affect correctness.
        time.sleep(2)
        quote = fetch_zoho_quote(quote_id, token)
        print(f"[{job_id}] Quote fields: {list(quote.keys())}")

        # ── Margin gate ───────────────────────────────────────
        _phase(context, job_id, "Checking vendor & opportunity margins...")
        margin = check_margin_gate(quote, token)
        if margin["blocked"]:
            print(f"[{job_id}] ⚠️  Margin gate NEEDS REVIEW — continuing anyway")

        # ── Format quote for Claude ───────────────────────────
        if _is_cancelled(context, job_id): return
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
        if _is_cancelled(context, job_id): return
        _phase(context, job_id, "Downloading PDF attachments...")
        # Cosmetic delay only — see note on the fetch-quote phase above; same
        # intra-network-vs-cross-cloud reasoning applies to these two calls.
        time.sleep(2)
        ppo_bytes, vq_bytes = _run_parallel(
            lambda: download_zoho_file(fid_ppo, token),
            lambda: download_zoho_file(fid_vq,  token),
        )

        if not is_valid_pdf(ppo_bytes):
            raise Exception(f"{PPO_PDF_FIELD} is not a valid PDF file.")
        if not is_valid_pdf(vq_bytes):
            raise Exception(f"{VQ_PDF_FIELD} is not a valid PDF file.")

        # ── Gemini extraction in parallel ─────────────────────
        if _is_cancelled(context, job_id): return
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
        if _is_cancelled(context, job_id): return
        result = run_comparison(zoho_text, ppo_text, vq_text, token, context=context, job_id=job_id)

        # ── Cancellation check ────────────────────────────────
        if _is_cancelled(context, job_id): return

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
        if _is_cancelled(context, job_id): return
        _phase(context, job_id, "Attaching report to Zoho quote...")
        attach_pdf_to_quote(quote_id, pdf_bytes, token, report_name)

        # ── Done ──────────────────────────────────────────────
        # CompareJobs.generated_at is a real Catalyst DateTime column, which
        # rejects Python's default isoformat() ("...T...+microseconds") —
        # confirmed live via INVALID_INPUT. Catalyst DateTime columns expect
        # "YYYY-MM-DD HH:MM:SS" (space-separated, no 'T', no offset, no
        # fractional seconds) — this still round-trips through
        # datetime.fromisoformat() in _ds_delete_old(), so no other change needed.
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _ds_write(context, job_id, {
            "status":       "done",
            "result_json":  json.dumps(result),
            "generated_at": generated_at,
            "quote_ref":    quote_ref
        })
        print(f"[{job_id}] ✅ Complete")

    except Exception as e:
        import traceback
        print(f"[{job_id}] ❌ {traceback.format_exc()}")
        if _ds_read(context, job_id).get("status") != "cancelled":
            _ds_write(context, job_id, {"status": "error", "error": str(e)})


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
    import google.generativeai as genai
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
    import google.generativeai as genai
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
def run_comparison(zoho_text: str, ppo_text: str, vq_text: str, token: str = "",
                    context=None, job_id: str = None) -> dict:
    import anthropic
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
        # Claude streaming is easily the single longest uninterruptible call
        # in the pipeline (can run tens of seconds). Poll cancellation every
        # ~2s of wall time (not every chunk — _is_cancelled hits DataStore,
        # too expensive to check per-token) so a mid-stream cancel actually
        # takes effect instead of running to completion regardless.
        last_check = time.time()
        for chunk in stream.text_stream:
            full_text += chunk
            if context is not None and job_id is not None and time.time() - last_check > 2:
                last_check = time.time()
                if _is_cancelled(context, job_id):
                    print(f"[{job_id}] ❌ Cancelled mid-stream — abandoning Claude call")
                    raise Exception("Cancelled by user")
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
# GENERATE PDF REPORT
#
# Built directly on ReportLab's Platypus API, NOT xhtml2pdf's HTML/CSS
# translation layer. xhtml2pdf has no flexbox support at all (every
# `display:flex` block in the old template silently collapsed into plain
# inline text — confirmed live, that's why the currency cards ran together
# with no spacing) and its table-layout width handling isn't reliable enough
# to keep columns inside the printable page area (confirmed live: the
# Status column ran off the page edge). ReportLab is already xhtml2pdf's own
# PDF-drawing backend (`pip show xhtml2pdf` lists `reportlab` as a direct
# dependency), so building against it directly costs zero extra memory —
# it just removes the unreliable translation step that was causing both bugs.
#
# Base-14 PDF fonts (Helvetica/Courier) don't include glyphs like ✓ ⚠ ↔ —
# rendering those silently drops the glyph. Deliberately using plain-ASCII
# equivalents throughout instead of embedding a Unicode font just for a
# handful of decorative symbols.
# ═════════════════════════════════════════════════════════════
def generate_pdf_report(result: dict, quote_subject: str, initiated_by: str = "") -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        HRFlowable, KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    print("Generating PDF report...")
    t0 = time.time()

    # ── Palette (matches the original design's hex values) ─────
    NAVY   = colors.HexColor("#1a1a2e")
    SLATE  = colors.HexColor("#374151")
    GRAY   = colors.HexColor("#6b7280")
    LGRAY  = colors.HexColor("#9ca3af")
    HAIR   = colors.HexColor("#f3f4f6")
    DASH   = colors.HexColor("#d1d5db")
    SHADE  = colors.HexColor("#f8f8f8")
    GREEN_BG, GREEN_TX, GREEN_BD = colors.HexColor("#d1fae5"), colors.HexColor("#065f46"), colors.HexColor("#6ee7b7")
    AMBER_BG, AMBER_TX, AMBER_BD = colors.HexColor("#fef3c7"), colors.HexColor("#92400e"), colors.HexColor("#fcd34d")
    RED_BG,   RED_TX,   RED_BD   = colors.HexColor("#fee2e2"), colors.HexColor("#991b1b"), colors.HexColor("#ef4444")
    NA_BG,    NA_TX               = colors.HexColor("#f3f4f6"), colors.HexColor("#9ca3af")

    PAGE_W = landscape(A4)[0] - 24 * mm  # usable width inside 12mm margins

    def status_style(status):
        """Mirrors the widget's own statusPill() classification."""
        if not status or status == "-":
            return NA_BG, NA_TX, "N/A"
        s = status.lower()
        if "mismatch"  in s: return RED_BG,   RED_TX,   "Mismatch"
        if "not found" in s: return AMBER_BG, AMBER_TX, "Not Found"
        if "review"    in s: return AMBER_BG, AMBER_TX, "Review"
        if "match"     in s: return GREEN_BG, GREEN_TX, "Match"
        return NA_BG, NA_TX, status

    # ── Styles ───────────────────────────────────────────────────
    title_style     = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=16, textColor=NAVY,
                                      leading=20, spaceAfter=4)
    subtitle_style  = ParagraphStyle("subtitle", fontName="Helvetica", fontSize=8, textColor=GRAY)
    card_title      = ParagraphStyle("card_title", fontName="Helvetica-Bold", fontSize=8, textColor=SLATE,
                                      spaceBefore=2, spaceAfter=3)
    th_style        = ParagraphStyle("th", fontName="Helvetica-Bold", fontSize=7, textColor=colors.white)
    td_style        = ParagraphStyle("td", fontName="Helvetica", fontSize=7.5, textColor=NAVY, leading=10)
    td_mono         = ParagraphStyle("td_mono", fontName="Courier", fontSize=7.5, textColor=NAVY, leading=10)
    td_gray         = ParagraphStyle("td_gray", fontName="Helvetica", fontSize=7, textColor=GRAY, leading=9)
    td_bold         = ParagraphStyle("td_bold", fontName="Helvetica-Bold", fontSize=7.5, textColor=SLATE)
    td_sub          = ParagraphStyle("td_sub", fontName="Helvetica", fontSize=6.5, textColor=GRAY, leftIndent=6)
    td_dash         = ParagraphStyle("td_dash", fontName="Courier", fontSize=7.5, textColor=DASH)
    col_hdr_style   = ParagraphStyle("col_hdr", fontName="Helvetica-Bold", fontSize=6, textColor=LGRAY)
    pill_style      = ParagraphStyle("pill", fontName="Helvetica-Bold", fontSize=6.5, leading=8, alignment=TA_CENTER)
    banner_title    = ParagraphStyle("banner_title", fontName="Helvetica-Bold", fontSize=10, textColor=NAVY)
    banner_detail   = ParagraphStyle("banner_detail", fontName="Helvetica", fontSize=7.5, textColor=SLATE, leading=11)
    overall_style   = ParagraphStyle("overall", fontName="Helvetica", fontSize=8, textColor=SLATE, leading=12)

    def pill(status):
        """Small fixed-width colored badge — a 1-cell Table so it can sit inside any other cell."""
        bg, tx, label = status_style(status)
        p = Paragraph(label, ParagraphStyle("pill_c", parent=pill_style, textColor=tx))
        t = Table([[p]], colWidths=[19 * mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), bg),
            ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return t

    def colored_block(flowables, bg, accent):
        """A full-width colored banner with a thick left accent bar, replacing the old
        `border-left: Npx solid ...; background: ...` div pattern."""
        t = Table([[f] for f in flowables], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), bg),
            ("LINEBEFORE", (0, 0), (0, -1), 3.5, accent),
            ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return t

    def section_title(text):
        return [Paragraph(text, card_title), HRFlowable(width=PAGE_W, thickness=1.2, color=HAIR, spaceAfter=4)]

    story = []

    # ── Header ───────────────────────────────────────────────────
    story.append(Paragraph("Procurement Analysis Report", title_style))
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    by_line = f"  |  Initiated by: {initiated_by}" if initiated_by else ""
    story.append(Paragraph(f"Quote: {quote_subject}  |  Generated: {generated_at}{by_line}", subtitle_style))
    story.append(Spacer(1, 4 * mm))

    # ── Margin gate banner ──────────────────────────────────────
    mg = result.get("margin_gate") or {}
    if mg.get("checked"):
        gm, vm = mg.get("gross_margin"), mg.get("vendor_margin")
        gm_s = f"{gm}%" if gm is not None else "—"
        vm_s = f"{vm}%" if vm is not None else "—"
        if mg.get("needs_review"):
            lines = [
                Paragraph("Margin Check — Needs Review",
                          ParagraphStyle("mnr_title", fontName="Helvetica-Bold", fontSize=9, textColor=AMBER_TX)),
                Paragraph(f"Gross Margin ({gm_s}) is less than Minimum Vendor Margin ({vm_s}) — flagged for review.",
                          ParagraphStyle("mnr_detail", fontName="Helvetica", fontSize=7.5,
                                         textColor=colors.HexColor("#78350f"))),
                Paragraph(f"Opportunity: {mg.get('opportunity_name') or '—'}   |   Vendor: {mg.get('vendor_name') or '—'}",
                          ParagraphStyle("mnr_stats", fontName="Courier", fontSize=7.5,
                                         textColor=colors.HexColor("#78350f"))),
            ]
            story.append(colored_block(lines, AMBER_BG, AMBER_BD))
        else:
            line = Paragraph(f"Margin check passed  —  Gross Margin: {gm_s}   |   Min Vendor Margin: {vm_s}",
                              ParagraphStyle("mp", fontName="Helvetica-Bold", fontSize=8, textColor=GREEN_TX))
            story.append(colored_block([line], GREEN_BG, GREEN_BD))
        story.append(Spacer(1, 4 * mm))

    # ── Currency overview ────────────────────────────────────────
    currencies = result.get("currencies_detected") or {}
    qc  = currencies.get("quote_currency") or "—"
    ppc = currencies.get("partner_po_currency") or "—"
    vqc = currencies.get("vendor_quote_currency") or "—"
    cur_notes = currencies.get("notes") or ""

    story.extend(section_title("CURRENCY OVERVIEW"))
    label_style = ParagraphStyle("cur_label", fontName="Helvetica-Bold", fontSize=6, textColor=LGRAY)
    value_style = ParagraphStyle("cur_value", fontName="Courier-Bold", fontSize=11, textColor=NAVY)
    gap = 4 * mm
    item_w = (PAGE_W - 2 * gap) / 3.0
    cur_table = Table(
        [[Paragraph("ZOHO QUOTE", label_style), "", Paragraph("PARTNER PO", label_style), "",
          Paragraph("VENDOR QUOTE", label_style)],
         [Paragraph(qc, value_style), "", Paragraph(ppc, value_style), "", Paragraph(vqc, value_style)]],
        colWidths=[item_w, gap, item_w, gap, item_w],
    )
    cur_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), HAIR), ("BACKGROUND", (2, 0), (2, -1), HAIR), ("BACKGROUND", (4, 0), (4, -1), HAIR),
        ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 4), ("BOTTOMPADDING", (0, 0), (-1, 0), 1),
        ("TOPPADDING", (0, 1), (-1, 1), 0), ("BOTTOMPADDING", (0, 1), (-1, 1), 5),
    ]))
    story.append(cur_table)
    if cur_notes:
        story.append(Spacer(1, 2))
        story.append(Paragraph(cur_notes, td_gray))
    story.append(Spacer(1, 5 * mm))

    # ── Document header validation ──────────────────────────────
    dhv = result.get("document_header_validation") or {}

    def dhv_row(label, fd):
        if not fd:
            return None
        return [Paragraph(label, td_bold), Paragraph(str(fd.get("zq_value") or "—"), td_mono),
                Paragraph(str(fd.get("pdf_value") or "—"), td_mono), pill(fd.get("status")),
                Paragraph(fd.get("note") or "", td_gray)]

    dhv_rows = [r for r in [
        dhv_row("Reseller", dhv.get("reseller")),
        dhv_row("Partner PO Ref", dhv.get("partner_po_ref")),
        dhv_row("Vendor", dhv.get("vendor")),
        dhv_row("Vendor Quote Ref", dhv.get("vendor_quote_ref")),
    ] if r]

    if dhv_rows:
        story.extend(section_title("DOCUMENT HEADER VALIDATION"))
        header_row = [Paragraph("Field", th_style), Paragraph("Zoho Quote Value", th_style),
                      Paragraph("PDF Extracted Value", th_style), Paragraph("Status", th_style),
                      Paragraph("Note", th_style)]
        widths = [35 * mm, 55 * mm, 55 * mm, 22 * mm, PAGE_W - 167 * mm]
        tbl = Table([header_row] + dhv_rows, colWidths=widths, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("LINEBELOW", (0, 0), (-1, -1), 0.5, HAIR),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 5 * mm))

    # ── Final call banner ────────────────────────────────────────
    fc = (result.get("final_call") or "").upper()
    if "CLEAR" in fc:  fc_bg, fc_bd = GREEN_BG, GREEN_BD
    elif "HOLD" in fc: fc_bg, fc_bd = RED_BG, RED_BD
    else:              fc_bg, fc_bd = AMBER_BG, AMBER_BD

    fc_lines = [Paragraph(result.get("final_call", "") or "", banner_title)]
    for d in (result.get("final_call_detail") or []):
        fc_lines.append(Paragraph(f"–  {d}", banner_detail))
    story.append(colored_block(fc_lines, fc_bg, fc_bd))
    story.append(Spacer(1, 5 * mm))

    # ── Section 1 — Three-way item matching ─────────────────────
    story.extend(section_title("SECTION 1 — THREE-WAY ITEM MATCHING"))

    def sku_header(r, i):
        num, code = str(r.get("num") or i + 1), r.get("sku") or "—"
        all_s = [r.get(f, "") for f in
                 ("zq_status", "vq_status", "ppo_status", "list_price_comparison_status", "buy_price_comparison_status")]
        worst = "match"
        for s in all_s:
            sl = (s or "").lower()
            if "mismatch" in sl: worst = "mismatch"; break
            if "review" in sl and worst != "mismatch": worst = "review"
        overall_status = {"mismatch": "Mismatch", "review": "Review", "match": "Match"}[worst]
        left = Paragraph(f'<font color="#9ca3af" size="7">{num}.</font>  <font color="white"><b>{code}</b></font>',
                          ParagraphStyle("sku_left", fontName="Courier", fontSize=8))
        t = Table([[left, pill(overall_status)]], colWidths=[PAGE_W - 25 * mm, 25 * mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), NAVY),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ("LEFTPADDING", (0, 0), (0, 0), 8), ("RIGHTPADDING", (1, 0), (1, 0), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return t

    def sku_detail(r):
        zqq  = str(r.get("zq_qty"))  if r.get("zq_qty")  is not None else "-"
        vqq  = str(r.get("vq_qty"))  if r.get("vq_qty")  is not None else "-"
        ppoq = str(r.get("ppo_qty")) if r.get("ppo_qty") is not None else "-"
        all_qty_match = all(
            ("match" in (r.get(f) or "").lower() and "mismatch" not in (r.get(f) or "").lower())
            for f in ["zq_status", "vq_status", "ppo_status"]
        )
        if all_qty_match:
            qty_status = pill("Match")
        else:
            qs = Table([
                [Paragraph("ZQ<->PPO", td_gray), pill(r.get("zq_status"))],
                [Paragraph("ZQ<->VQ",  td_gray), pill(r.get("vq_status"))],
                [Paragraph("PPO<->VQ", td_gray), pill(r.get("ppo_status"))],
            ], colWidths=[22 * mm, 19 * mm])
            qs.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 1), ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]))
            qty_status = qs

        rows = [
            ["", Paragraph("ZQ", th_style), Paragraph("PPO", th_style), Paragraph("VQ", th_style),
             Paragraph("Status", th_style)],
            [Paragraph("Qty", td_bold), Paragraph(zqq, td_mono), Paragraph(ppoq, td_mono), Paragraph(vqq, td_mono),
             qty_status],
            [Paragraph("Price", td_bold), Paragraph("ZQ", col_hdr_style), Paragraph("PPO", col_hdr_style),
             Paragraph("VQ", col_hdr_style), ""],
            [Paragraph("List Price", td_sub), Paragraph(str(r.get("list_price_zq") or "-"), td_mono),
             Paragraph(str(r.get("partner_ppo_price_original") or "-"), td_mono),
             Paragraph("—", td_dash), pill(r.get("list_price_comparison_status"))],
            [Paragraph("Buy Price", td_sub), Paragraph(str(r.get("buy_price_zq") or "-"), td_mono),
             Paragraph("—", td_dash), Paragraph(str(r.get("vendor_quote_price") or "-"), td_mono),
             pill(r.get("buy_price_comparison_status"))],
        ]
        note = (r.get("notes") or "").strip()
        span_style = []
        if note:
            rows.append([Paragraph("Notes", td_bold), Paragraph(note, td_gray), "", "", ""])
            span_style = [("SPAN", (1, 5), (4, 5))]

        label_w, val_w = 22 * mm, 32 * mm
        widths = [label_w, val_w, val_w, val_w, PAGE_W - label_w - 3 * val_w]
        tbl = Table(rows, colWidths=widths)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), SLATE),
            ("BACKGROUND", (0, 2), (-1, 4), SHADE),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LINEBELOW", (0, 0), (-1, -2), 0.5, HAIR),
        ] + span_style))
        return tbl

    for i, r in enumerate(result.get("matching_table", [])):
        story.append(KeepTogether([sku_header(r, i), sku_detail(r), Spacer(1, 2.5 * mm)]))

    story.append(Spacer(1, 3 * mm))

    # ── Subtotal validation ──────────────────────────────────────
    sv = result.get("subtotal_validation") or {}

    def sv_row(d):
        if not d:
            return None
        pu, ov, pr = d.get("pdf_subtotal_usd"), d.get("opportunity_value"), d.get("pdf_subtotal")
        pus = f"${pu:,.2f}" if pu is not None else "—"
        ovs = f"${ov:,.2f}" if ov is not None else "—"
        prs = f"{pr:,.2f} {d.get('pdf_currency') or ''}".strip() if pr is not None else "—"
        return [Paragraph(d.get("label", ""), td_bold), Paragraph(prs, td_mono), Paragraph(pus, td_mono),
                Paragraph(ovs, td_mono), pill(d.get("status"))]

    sv_rows = [r for r in [sv_row(sv.get("partner_po")), sv_row(sv.get("vendor_quote"))] if r]
    if sv_rows:
        story.extend(section_title("SUBTOTAL VALIDATION (VS OPPORTUNITY)"))
        header_row = [Paragraph("Check", th_style), Paragraph("PDF Subtotal", th_style),
                      Paragraph("Converted (USD)", th_style), Paragraph("Opportunity Value", th_style),
                      Paragraph("Status", th_style)]
        widths = [75 * mm, 45 * mm, 40 * mm, 40 * mm, PAGE_W - 200 * mm]
        tbl = Table([header_row] + sv_rows, colWidths=widths)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("LINEBELOW", (0, 0), (-1, -1), 0.5, HAIR),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 2 * mm))
        story.append(Table([[Paragraph("Overall:", td_bold), pill(sv.get("overall_status"))]],
                            colWidths=[20 * mm, 22 * mm]))
        story.append(Spacer(1, 5 * mm))

    # ── Section 2 — Summary ──────────────────────────────────────
    story.extend(section_title("SECTION 2 — SUMMARY"))

    def summary_list(items, accent, empty_text):
        if not items:
            return [Paragraph(empty_text, ParagraphStyle("empty", fontName="Helvetica-Oblique", fontSize=7.5, textColor=GRAY))]
        out = []
        for it in items:
            t = Table([[Paragraph(it, ParagraphStyle("sitem", fontName="Helvetica", fontSize=7.5,
                                                       textColor=SLATE, leftIndent=4))]], colWidths=[PAGE_W])
            t.setStyle(TableStyle([
                ("LINEBEFORE", (0, 0), (0, 0), 2.5, accent),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]))
            out.append(t)
            out.append(Spacer(1, 1))
        return out

    story.append(Paragraph("Must Resolve Before Processing",
                            ParagraphStyle("lbl_red", fontName="Helvetica-Bold", fontSize=8, textColor=RED_BD)))
    story.extend(summary_list(result.get("must_resolve") or [], RED_BD, "None — all items cleared"))
    story.append(Spacer(1, 3 * mm))

    story.append(Paragraph("Needs Human Review",
                            ParagraphStyle("lbl_amber", fontName="Helvetica-Bold", fontSize=8, textColor=colors.HexColor("#f59e0b"))))
    story.extend(summary_list(result.get("needs_review") or [], colors.HexColor("#f59e0b"), "None — no items flagged for review"))
    story.append(Spacer(1, 3 * mm))

    unmatched = result.get("unmatched_items") or []
    if unmatched:
        story.append(Paragraph("Unmatched Items",
                                ParagraphStyle("lbl_red2", fontName="Helvetica-Bold", fontSize=8, textColor=RED_BD)))
        story.append(Paragraph(", ".join(unmatched),
                                ParagraphStyle("tags", fontName="Courier", fontSize=7.5, textColor=RED_TX)))
        story.append(Spacer(1, 3 * mm))

    story.append(Paragraph("Overall",
                            ParagraphStyle("lbl_green", fontName="Helvetica-Bold", fontSize=8, textColor=colors.HexColor("#10b981"))))
    story.append(Paragraph(result.get("overall_summary", "") or "", overall_style))

    # ── Build ────────────────────────────────────────────────────
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(A4),
        leftMargin=12 * mm, rightMargin=12 * mm, topMargin=12 * mm, bottomMargin=12 * mm,
        title="Procurement Analysis Report",
    )
    doc.build(story)
    pdf_bytes = buf.getvalue()
    print(f"PDF generated: {len(pdf_bytes)} bytes in {time.time()-t0:.1f}s")
    return pdf_bytes
