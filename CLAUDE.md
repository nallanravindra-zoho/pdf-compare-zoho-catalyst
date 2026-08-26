# AI Document Comparison — Claude Code Context

## What this project does

A Zoho CRM widget that compares three procurement documents for a quote record:
- **ZQ** — Zoho Quote (fetched from CRM API)
- **PPO** — Partner Purchase Order (PDF attached to the quote)
- **VQ** — Vendor Quote (PDF attached to the quote)

The widget opens from a button on a Zoho Quotes page. It:
1. Downloads both PDFs from Zoho filestore
2. Extracts line items + header fields using **Gemini AI**
3. Compares all three documents (SKU, qty, price, header fields) using **Claude AI**
4. Generates a PDF report and attaches it back to the Zoho quote record
5. Renders results in an accordion UI inside the Zoho widget

---

## Repository structure

```
doc-compare-catalyst/
├── functions/
│   └── document_compare/
│       ├── index.py          ← full backend (Catalyst entry point)
│       └── requirements.txt  ← anthropic, google-generativeai, xhtml2pdf, requests
├── catalyst.json             ← Catalyst project config
├── .gitignore
└── CLAUDE.md                 ← this file
```

The Zoho CRM widget (`index.html`) lives separately — uploaded directly to
Zoho CRM → Setup → Developer Space → Widgets. It is NOT deployed via Catalyst.

---

## Architecture

### Before (Cloud Run)
```
Zoho CRM widget (index.html)
    → ZOHO.CRM.HTTP.post → Cloud Run (app.py / FastAPI)
    → background thread → Gemini + Claude APIs
    → poll /job-status/ → return result
    → attach PDF to quote
```

### Now (Catalyst)
```
Zoho CRM widget (index.html)  ← unchanged
    → ZOHO.CRM.HTTP.post → Catalyst Advanced I/O Function (index.py)
    → synchronous execution (up to 540s) → Gemini + Claude APIs
    → DataStore writes for progress → poll /job-status/
    → return result → attach PDF to quote
```

### Key architectural differences from Cloud Run version
| Cloud Run (app.py) | Catalyst (main.py) |
|---|---|
| `get_access_token()` — OAuth refresh | Same refresh-token flow, restored in `_get_token()` — see §"Zoho CRM auth" below |
| `jobs = {}` in-memory dict | Catalyst DataStore table `CompareJobs`, read/written via ZCQL (see DataStore section) |
| FastAPI + BackgroundTasks | `handler(request)` single function |
| `is_cancelled(job_id)` reads dict | `_is_cancelled(context, job_id)` reads DataStore |
| WeasyPrint for PDF | xhtml2pdf (pure Python, no C deps) |
| Auto-cancel watcher thread | Not needed — Catalyst enforces 540s timeout |

---

## Catalyst function entry point

`request` is a real **Flask `Request`** object (Catalyst runs Advanced I/O functions
on Flask under the hood) — it has no `.catalyst` or `.body` attribute, despite what
older comments in this file used to say.

```python
def handler(request):
    context = zcatalyst_sdk.initialize()       # Catalyst context — DataStore/ZCQL access
    path    = str(request.url)
    method  = request.method
    body    = request.get_data(as_text=True)   # NOT request.body — Flask has no such attr
```

Responses go back via `make_response(jsonify(body), status)` (Flask), not any
`catalyst.response()` API — that package doesn't exist in the runtime.

### Endpoints (all routed inside handler)
| Method | Path | Function |
|---|---|---|
| POST | /analyze-quote | `_handle_analyze` — creates the job, spawns `_run_analysis_pipeline` on a background thread, returns `job_id` immediately (does NOT run the pipeline inline — see DEPLOYMENT.md changelog #11) |
| GET | /job-status/{job_id} | `_handle_job_status` — poll for progress |
| GET | /check-report/{quote_id} | `_handle_check_report` — existing report check |
| POST | /cancel-job/{job_id} | `_handle_cancel` — cancel a running job |

---

## Zoho CRM auth — NOT the same as Catalyst's own credential

`zcatalyst_sdk.initialize()` gives you `context.credential.token()`, but that token
authenticates the caller to **the Catalyst project itself** (DataStore/Cache/Filestore).
It carries **no Zoho CRM API scope** — confirmed live, calling `zohoapis.com/crm/v3/...`
with it 401s with `OAUTH_SCOPE_MISMATCH`. There is no Catalyst-native way to get a
CRM-scoped token just because the function backs a CRM widget.

`_get_token()` therefore restores the exact refresh-token exchange the Cloud Run
`app.py` used: `CLIENT_ID` / `CLIENT_SECRET` / `REFRESH_TOKEN` → POST
`{ZOHO_ACCOUNTS_URL}/oauth/v2/token` → cached access token (module-level cache,
reused across invocations on a warm worker, refreshed ~60s before `expires_in`).
All `zohoapis.com/crm/v3/...` calls go through `_raise_for_zoho()` instead of a bare
`r.raise_for_status()`, so a future auth/scope problem shows Zoho's actual JSON error
body (`{"code": "...", "message": "..."}`) instead of a bare `401 Client Error`.

---

## Catalyst DataStore

Table name: **CompareJobs**

| Column | Type | Purpose |
|---|---|---|
| `job_id` | Single Line | UUID — lookup key |
| `status` | Single Line | processing / done / error / cancelled |
| `phase` | Single Line | Phase label shown in widget spinner |
| `result_json` | Multi Line | Full Claude JSON output |
| `generated_at` | Datetime | `"YYYY-MM-DD HH:MM:SS"` — NOT `datetime.isoformat()`, which this column rejects live with `INVALID_INPUT` (no `T`, no offset, no fractional seconds) |
| `quote_ref` | Single Line | Quotation_Reference from ZQ |
| `error` | Multi Line | Error message when status=error |

Catalyst's `Table` API has **no filter-by-column read/update/delete** — rows are
addressed by a system `ROWID`, not by `job_id`. `_ds_read()` looks a row up by
`job_id` via ZCQL (`context.zcql().execute_query("SELECT * FROM CompareJobs WHERE
job_id = '...'")`); `_ds_write()` upserts by finding the row's `ROWID` first, then
calling `table.update_row({"ROWID": ..., **fields})`, or `insert_row()` if none
exists yet. `job_id` is validated against a UUID-shaped regex before going into the
ZCQL string (it reaches `_ds_read`/`_ds_write` from the `job-status`/`cancel-job`
URL path, i.e. caller-controlled) — don't remove that guard.

DataStore helpers: `_ds_write`, `_ds_read`, `_ds_delete_old` (24hr housekeeping,
via `table.get_iterable_rows()`)

---

## Environment variables (set in Catalyst Console)

```
CLAUDE_API_KEY     ← Anthropic API key
GEMINI_API_KEY     ← Google AI API key
PPO_PDF_FIELD      ← Zoho field API name for Partner PO PDF (default: PARTNER_PO_PDF)
VQ_PDF_FIELD       ← Zoho field API name for Vendor Quote PDF (default: VQ_PDF)
CLIENT_ID          ← Zoho CRM OAuth client id (same one Cloud Run app.py used)
CLIENT_SECRET      ← Zoho CRM OAuth client secret
REFRESH_TOKEN      ← Zoho CRM OAuth refresh token (needs CRM API scope, e.g.
                       ZohoCRM.modules.ALL, ZohoCRM.files.ALL)
ZOHO_ACCOUNTS_URL  ← optional, default: https://accounts.zoho.com — change only
                       if the CRM org is on a different data center (.eu/.in/.au/...)
```

`CLIENT_ID` / `CLIENT_SECRET` / `REFRESH_TOKEN` ARE required — see "Zoho CRM auth"
section above. An earlier version of this file said not to add these, based on an
unverified assumption that Catalyst forwards a CRM-scoped token automatically;
that assumption was wrong (confirmed live via `OAUTH_SCOPE_MISMATCH`).

---

## AI prompts

Both prompts live in **Zoho CRM → AIPrompts module** (custom module), not in code.
Loaded at runtime via `load_gemini_prompt(token)` / `load_claude_prompt(token)`.
5-minute cache with stale fallback.

### Gemini prompt (field: GEMINI_PROMPT)
Extracts from each PDF:
- **Header fields**: `reseller_name`, `partner_po_ref`, `partner_po_subtotal` (PPO)
  and `vendor_name`, `vendor_quote_ref`, `vendor_quote_subtotal` (VQ)
- **Line items**: `line_num`, `sku`, `description`, `quantity`, `list_unit_price`

Key rules:
- Returns `{ "header": {...}, "line_items": [...] }` JSON format
- `list_unit_price` = post-discount net price (not list/gross price)
- Subtotals = pre-tax, post-discount (anchor from Final Total, subtract tax only)
- Distributor blocklist: never return CyberKnight entities as reseller/vendor name
- Logo + footer + email domain fallback for company name extraction
- PERSISTENCE RULE: must check all sources before returning null

### Claude prompt (field: CLAUDE_PROMPT)
Three-way document comparison producing structured JSON:
- `currencies_detected` — ZQ/PPO/VQ currency codes + conversion notes
- `document_header_validation` — reseller, partner_po_ref, vendor, vendor_quote_ref match
- `matching_table` — per-SKU rows with qty + price status for each document pair
- `unmatched_items`, `needs_review`, `must_resolve`, `overall_summary`, `final_call`

Key rules (prompt-level, not in code):
- **SKU is the sole matching key** — never match by description across documents
- Y1/Y2/Y3 suffix exception: same base SKU with annual-split suffix → Needs Review
- Bundling independence: PPO annual split never affects `vq_status`
- Quantity equality rule: Match requires numerically equal consolidated quantities
- Zero-price subscription duration rows (qty=months, price=0) → separate, never matched
- Bundling price rule: if `zq_status = Needs Review` due to bundling → price = Needs Review
- Notes field: max 25 words, facts only, no calculations

### Claude model
`claude-sonnet-4-6` — hardcoded in `run_comparison()`

---

## Zoho CRM field references

### Quote fields used
| API Name | Purpose |
|---|---|
| `Currency` | ZQ currency (quote_currency) |
| `Partner_PO_Currency` | PPO currency |
| `Vendor_Quote_Currency` | VQ currency |
| `Exchange_Rate` | ZQ exchange rate |
| `Reseller` | Partner/reseller company name |
| `Partner_PO_Ref` | Partner PO reference number |
| `Vendor` | Vendor company name |
| `Vendor_Quote_Ref` | Vendor quote reference number |
| `Quotation_Reference` | Used for PDF report filename |
| `PARTNER_PO_PDF` | PPO PDF attachment field |
| `VQ_PDF` | VQ PDF attachment field |
| `Deal_Name` | Linked opportunity (for margin gate) |
| `Quoted_Items` | Line items array |

### Quoted_Items fields
`Product_Name.Product_Code`, `Product_Name.name`, `Description`,
`Quantity`, `Buy_Price`, `List_Price`

Note: `List_Price` from Zoho API may have float precision artifacts
(e.g. 8844.086021505 instead of 8844.09) due to multi-currency
back-calculation. Always `round(value, 2)` before sending to Claude.

### Opportunity fields (margin gate)
`GrossMarginCalc`, `Amount_in_USD`, `Net_to_Vendor`

### Vendor fields (margin gate)
`Min_Acceptable_Margin`

---

## Pipeline flow (inside _run_analysis_pipeline — runs on a background thread
spawned by _handle_analyze, NOT inline in the HTTP request; see "Zoho CRM auth"
sibling section below and DEPLOYMENT.md changelog #11 for why)

```
1. _get_token(context)
2. load_gemini_prompt + load_claude_prompt  ← fail fast if blank
3. fetch_zoho_quote(quote_id, token)
4. check_margin_gate(quote, token)          ← non-blocking, result attached to output
5. format_zoho_quote(quote)                 ← text block for Claude
6. Validate PPO + VQ file_Id fields exist
7. Parallel download: ppo_bytes + vq_bytes  ← _run_parallel() (threading.Thread)
8. is_valid_pdf() check on both
9. Parallel Gemini: ppo_text + vq_text      ← _run_parallel() (threading.Thread)
10. run_comparison(zoho_text, ppo_text, vq_text, token)  ← Claude streaming
11. check_subtotal_validation(quote, margin, ppo_header, vq_header)
12. generate_pdf_report(result, subject, initiated_by)  ← xhtml2pdf
13. attach_pdf_to_quote(quote_id, pdf_bytes, token, report_name)
14. _ds_write status=done + result_json
```

Each step writes its phase to DataStore via `_phase(context, job_id, "...")` so
the widget's poll loop shows live progress.

---

## Widget (index.html)

Uploaded to Zoho CRM → Setup → Developer Space → Widgets.
Not part of the Catalyst project — deployed separately.

### BASE_URL (line ~288)
```javascript
var BASE_URL = "https://<your-project>.catalystserverless.com/server/document_compare";
```

### Widget flow
1. PageLoad → `showRunConfirmation(quoteId)`
2. User clicks OK → `checkExistingReport(quoteId)`
3. If no existing → `runAnalysis(quoteId)` → POST /analyze-quote → gets job_id
4. `pollJobStatus(job_id, ...)` every 3s → GET /job-status/{job_id}
5. On status=done → `render(data)` → accordion UI

### UI components
- **Currency Overview card** — 3 currency pills
- **Document Header Validation card** — 4-row table (reseller, PO ref, vendor, quote ref)
- **Final Call banner** — green/amber/red based on CLEAR/QUERY/HOLD
- **Section 1 — Three-Way Item Matching** — accordion list, one card per SKU
  - Collapsed: SKU code + overall worst-case pill
  - Expanded: Qty row (single Match or 3 labelled pills) + Price rows (List/Buy split)
- **Section 2 — Summary** — must resolve / needs review / unmatched / overall

---

## PDF report

Generated by `generate_pdf_report()` using xhtml2pdf.
Attached to Zoho quote as `DOC_Compare_{safe_ref}.pdf`.
Landscape A4. Contains same sections as widget (static, fully expanded).

---

## Deployment

Full step-by-step guide (first-time setup, env vars, the deploy-wipes-Console-vars
footgun, log checking, changelog of every deployment-affecting fix) lives in
**[DEPLOYMENT.md](DEPLOYMENT.md)** — read that before deploying, not just this file.

Quick reference:

```bash
catalyst deploy --only functions:doc_compare   # deploy this function only
catalyst serve                                  # local dev/test
```

`catalyst logs:get` is **not a real command** in the current CLI (v1.27.0) —
check logs in the Catalyst Console instead (Console → project → Functions →
doc_compare → Logs).

### Catalyst project details
- Project: **Project-Rainfall**
- Project type: Advanced I/O Function
- Function name: **doc_compare** (per `functions/doc_compare/catalyst-config.json`)
- Runtime: `python_3_12`
- Timeout: 540s (configured in Catalyst Console)
- Min instances: 1 (recommended — avoids cold start)

---

## Common issues & fixes

| Symptom | Cause | Fix |
|---|---|---|
| `libpango` error | WeasyPrint needs C libs | Use xhtml2pdf — already done |
| `handler() missing argument 'event'` | Wrong handler signature | `handler(request)` not `handler(context, event)` |
| Reseller name = null | Gemini misidentifying document type | Check prompt persistence rule + blocklist in CRM |
| `vendor_quote_price: null` | Field name mismatch | Claude uses `list_unit_price` from Gemini → `vendor_quote_price` in JSON |
| List price has 12 decimal places | Zoho float artifact from multi-currency | `round(raw_list, 2)` in `format_zoho_quote` |
| ZQ↔VQ = Review despite same qty | ppo_status bleeding into vq_status | Bundling independence rule in Claude prompt |
| Different SKUs consolidated | Description-based cross-matching | Rule 0 in Claude prompt: SKU is sole matching key |
| Job not found after redeploy | DataStore row gone (dev redeploy wipes state) | Redeploy mid-job; just re-run |
| `405 Method Not Allowed` | Path routing issue | Check `"analyze-quote" in path` not `path.endswith(...)` |
| `'Request' object has no attribute 'catalyst'` | `request` is a Flask `Request`, not a Catalyst-specific type | `context = zcatalyst_sdk.initialize()`, not `request.catalyst` |
| `'Request' object has no attribute 'body'` | Same — Flask `Request` has no `.body` | `request.get_data(as_text=True)` |
| `'CatalystApp' object has no attribute 'get_token'` | No such method exists on the SDK's app object | `context.credential.token()` returns `(cred_type, token)` — but see below, that token still isn't CRM-scoped |
| `401 ... OAUTH_SCOPE_MISMATCH` calling `zohoapis.com/crm/...` | `context.credential.token()` is scoped to Catalyst's own services only | Use `_get_token()`'s restored `CLIENT_ID`/`CLIENT_SECRET`/`REFRESH_TOKEN` flow — see "Zoho CRM auth" section |
| DataStore write/read silently does nothing or errors | `table.update_row()`/`.get_rows()` used with a criteria dict — that API doesn't exist | Look up by `job_id` via ZCQL, update/delete by `ROWID` — see "Catalyst DataStore" section |
| `[DataStore] write error: INVALID_INPUT ... generated_at. datetime value expected` | `generated_at` is a real Catalyst `Datetime` column, not text — `datetime.now().isoformat()` (has a `T`, fractional seconds, no space) is rejected | Use `datetime.now().strftime("%Y-%m-%d %H:%M:%S")` — Catalyst `Datetime` columns want that exact space-separated, no-offset format |
| `Worker (pid:...) was sent SIGKILL! Perhaps out of memory?` | Function memory too low for this dependency footprint (`google-generativeai`'s grpc/protobuf/cryptography chain + `anthropic` + PDF byte buffers) | `catalyst functions:config doc_compare --memory 512` (or higher) — not a code bug |
| Widget spinner frozen on a stale phase forever | A `SIGKILL`/crash can't be caught by Python — the job's DataStore row simply stops being updated, so the widget keeps showing the last real phase it saw | Fix the underlying crash (usually the OOM above); the phase-tracking itself isn't broken, it's accurately reporting a dead job |
| `check-report`/`job-status` slow (~20s+) vs. a few seconds on Cloud Run | `anthropic`/`google.generativeai`/`xhtml2pdf` were imported at module top-level, so every cold worker paid their full import cost (2.7s+ for `google.generativeai` alone) even for endpoints that never touch Gemini/Claude/PDF | Already fixed — those three are now imported lazily inside the one function each needs, not at the top of the file |
| Widget spinner stuck on the first phase, then "Execution Time Exceeded" at the end, even though the job actually completed; `/cancel-job` did nothing | `_handle_analyze` ran the whole pipeline inline in one HTTP request (up to 540s), but the widget expects that endpoint to return fast with a `job_id` and poll `/job-status` separately, like Cloud Run's `BackgroundTasks` did | Already fixed — `_handle_analyze` now spawns `_run_analysis_pipeline` on a background thread and returns immediately. **Needs live verification that the thread survives past the response on Catalyst's runtime** — see DEPLOYMENT.md changelog #11 |
| `RuntimeError: cannot schedule new futures after interpreter shutdown` (from `ex.submit()` in the PDF-download or Gemini-extraction block) | `concurrent.futures.ThreadPoolExecutor` shares ONE process-wide atexit-driven shutdown flag across every executor in the process — once it fires (e.g. a prior/abandoned invocation's worker teardown on Catalyst's warm-process reuse), it permanently breaks `.submit()` for every future request routed to that same warm worker, unrelated code included. This is NOT a "Catalyst bans threading" restriction — plain `threading.Thread` has no such shared flag and isn't affected | Already fixed — see `_run_parallel()` below, which uses raw `threading.Thread` instead of `ThreadPoolExecutor` for the same real parallelism without this fragility |

---

## What NOT to change

- `run_comparison()` — Claude model is `claude-sonnet-4-6`, `max_tokens=32000`, `temperature=0`
- Prompt loading — always from CRM AIPrompts module, never hardcoded
- `_sanitise_prompt()` — strips invisible unicode that CRM editor injects
- The `round(value, 2)` on `Buy_Price` and `List_Price` — Zoho float bug
- `_attach_via_filestore()` fallback — needed when direct attachment returns 400
- `_get_token()`'s refresh-token flow — do NOT swap it back for `context.credential.token()`;
  that token has no Zoho CRM API scope (confirmed live via `OAUTH_SCOPE_MISMATCH`)
- The ZCQL lookup in `_ds_read()`/`_ds_write()` — Catalyst's `Table` API has no
  filter-by-column call, only `get_row(ROWID)`; don't "simplify" it back to a
  criteria-dict `get_rows()`/`update_row()` call, that API doesn't exist
- `_raise_for_zoho()` on every `zohoapis.com` call — a bare `r.raise_for_status()`
  drops Zoho's actual error body, which is the only thing that made the auth
  scope bug diagnosable
- `_run_parallel()` (raw `threading.Thread`) for the PDF-download and
  Gemini-extraction parallel blocks — do NOT swap it back for
  `concurrent.futures.ThreadPoolExecutor`; that caused a live
  "cannot schedule new futures after interpreter shutdown" crash on Catalyst's
  warm-worker reuse model (see Common issues table). This is about
  `ThreadPoolExecutor`'s specific shared-atexit-flag fragility, not a ban on
  parallelism — `_run_parallel()` still runs both tasks concurrently on real
  OS threads with no latency cost
- The `strftime("%Y-%m-%d %H:%M:%S")` on `generated_at` in `_handle_analyze()` —
  do NOT swap it back for `datetime.now().isoformat()`; the `Datetime` column
  rejects that live with `INVALID_INPUT`
- The lazy `import anthropic` / `import google.generativeai as genai` /
  `from xhtml2pdf import pisa` inside `run_comparison()` /
  `get_gemini_model()` + `extract_pdf_gemini()` / `generate_pdf_report()` —
  do NOT hoist these back to module top-level; that made every cold worker
  pay their import cost (`google.generativeai` alone measured 2.7s+) even for
  `/job-status` and `/check-report`, which never touch any of the three
- `_handle_analyze`'s split into "create job + spawn background thread + return
  immediately" vs. `_run_analysis_pipeline` (the actual work) — do NOT collapse
  these back into one synchronous function; that held the widget's HTTP client
  open past its own timeout and starved `/cancel-job`/`/job-status` of a free
  worker while `/analyze-quote` was in flight. See DEPLOYMENT.md changelog #11.

---

## Git workflow

```bash
# Feature branch off develop
git checkout develop
git checkout -b feature/your-feature-name

# After changes
git add .
git commit -m "feat/fix: description"
git push origin feature/your-feature-name

# Merge to develop, then main
git checkout develop && git merge feature/your-feature-name
git checkout main && git merge develop
git push origin main

# Deploy
catalyst deploy
```

---

## Repo
https://github.com/nallanravindra-zoho/doc-compare-catalyst
