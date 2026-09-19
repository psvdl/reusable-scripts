# Fabric notebook source
# METADATA ********************
# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # REST API → Bronze Lakehouse
#
# Metadata-driven notebook that extracts data from a REST API and lands it in the
# **bronze** layer (raw JSON + flattened Parquet). Currently supports two source
# systems, selected purely through parameters:
#
# | | Epicor Kinetic (OData) | AlayaCare |
# |---|---|---|
# | Auth | Basic Auth + `X-API-Key` header | Basic Auth only (username = public key, password = private key) |
# | `json_data_key` | `value` | `items` |
# | `pagination_type` | `next_url` (`@odata.nextLink`) | `page_number` (`?page=N&count=M`, stop at `total_pages`) |
# | Full-page fallback | **Watermark keyset** — `$filter=<prop> ge/gt <max>` (Epicor ignores `$skip`) | n/a (page numbers are native) |
# | `incremental_filter_style` | `odata_filter` | `query_param` (e.g. `start_date_from=`) |
# | Multi-level endpoints | n/a | **Parent-child full load** — `parent_api_url` + a `{placeholder}` in `api_url` (billing periods → invoices); **three-level** adds `middle_api_url` (periods → invoices → invoice details); only the deepest level lands, all-or-nothing on limit |
# | Bounded date-range endpoints | n/a | **Date-window full load** — `window_param_from/to` + `window_start_date` + `window_days` (forms submissions: max 14 days per call); all-or-nothing on limit |
#
# ## Security
# All credentials are read at runtime from Azure Key Vault via
# `notebookutils.credentials.getSecret(vault_url, secret_name)` — never hardcode them.
# The identity running the notebook needs **Get** permission on the vault.
# https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-credentials
#
# ## Epicor notes
# * Server-driven paging via `@odata.nextLink` is followed when present.
# * Epicor caps pages at 100 records and (on many tenants/services) never emits
#   `@odata.nextLink` and silently ignores `$skip`. When a full page returns
#   without a nextLink, the notebook falls back to **watermark keyset pagination**:
#   take the max `api_filter_property` value of the batch and re-query with
#   `$filter=<prop> ge <max>` (Date) or `gt <max>` (Number), repeating until an
#   empty/short page. Requires `api_filter_property` + `delta_format`.

# CELL ********************

%run /EnvSettings

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run /commonTransforms
%run /DeltaLakeFunctions

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook Parameters

# PARAMETERS CELL ********************

# -------------------------------------------------------------------------
# API CONFIG
# Epicor:    "https://<host>/<instance>/api/v2/odata/<company>/Erp.BO.CustomerSvc/Customers"
# AlayaCare: "https://<tenant>/ext/api/v2/employees/employees"
# Source-specific query options go straight into the URL
# (OData $select/$top/$filter for Epicor; filter/start_date_from for AlayaCare)
# -------------------------------------------------------------------------
api_url = None

# -------------------------------------------------------------------------
# KEY VAULT — Basic Auth credentials (required)
# key_vault_name accepts the vault name ("kv-prod-01") or full URL
# ("https://kv-prod-01.vault.azure.net/")
# -------------------------------------------------------------------------
key_vault_name = None
username_secret_name = None     # Epicor username / AlayaCare public key
password_secret_name = None     # Epicor password / AlayaCare private key
api_key_secret_name = None      # OPTIONAL — only for sources needing an X-API-Key
                                # header (Epicor). Leave None for AlayaCare.

# -------------------------------------------------------------------------
# JSON / PAGINATION CONFIG
# -------------------------------------------------------------------------
json_data_key = None            # record array: "value" (Epicor) | "items" (AlayaCare)
pagination_type = None          # "next_url" (Epicor) | "page_number" (AlayaCare) | "none"
pagination_next_url_key = None  # next-URL key, e.g. "@odata.nextLink" (next_url only)
page_number_param = "page"      # page query param (page_number only)
page_size_param = "count"       # page-size query param (page_number only)
page_size = "100"               # page size (page_number only)
total_pages_key = "total_pages" # response key with total page count (page_number only)
max_pages = "100"               # safety stop
request_timeout_seconds = "60"
max_retries = "5"

# -------------------------------------------------------------------------
# API RATE-LIMIT (DAILY QUOTA) HANDLING
# When the API's daily limit is reached, the notebook stops gracefully, writes
# the records fetched so far and returns the high watermark — the next run
# resumes from there (read last_watermark from your config table).
# -------------------------------------------------------------------------
api_limit_http_codes = "429"    # comma-separated status codes meaning "daily limit reached"
api_limit_error_substring = None  # optional text in an error body meaning the same,
                                  # e.g. "calls per day" (some APIs return 400/500 instead of 429)
# What to do with the watermark when a run is cut short by the daily limit on a
# source WITHOUT guaranteed date ordering (e.g. AlayaCare — its Swagger specs
# document no sort parameter):
#   "hold_watermark" (default, safe) → return last_watermark unchanged; the next
#                                      run re-fetches the whole range (no gaps possible)
#   "max_fetched"                    → advance to the max date fetched; ONLY safe if
#                                      you have verified the endpoint returns rows
#                                      in ascending date order on your tenant
partial_run_unordered_policy = "hold_watermark"

# -------------------------------------------------------------------------
# ONELAKE DESTINATION (bronze)
# -------------------------------------------------------------------------
destination_raw_file_system = None   # usually "Files"
destination_raw_file_folder = None   # e.g. "raw_bronze/Epicor/GLJrnDtlSvc/GLJrnDtls/2023-04"
destination_raw_file = None          # e.g. "GLJrnDtls-04-16_035316.parquet"
write_raw_json = "True"              # also persist the raw API payload (raw_json_bronze/...)

# -------------------------------------------------------------------------
# INCREMENTAL / WATERMARK CONFIG
# -------------------------------------------------------------------------
delta_format = None             # "Date" | "Number" — data type of api_filter_property
last_watermark = None
# How the watermark is sent to the API:
#   "odata_filter" → $filter=<prop> ge/gt <watermark>   (Epicor)
#   "query_param"  → <prop>=<watermark>                 (AlayaCare, e.g. start_date_from)
incremental_filter_style = None
# The API-side watermark property, e.g. "PostedDate" / "ChangeDate" (Epicor Date),
# "SysRevID" (Epicor Number) or "start_date_from" (AlayaCare).
# REQUIRED for Epicor streams: without it there is no fallback when Epicor omits
# @odata.nextLink (it caps at 100 records and ignores $skip).
api_filter_property = None
# The payload column holding the watermark value — defaults to api_filter_property.
# Set it when the two differ, e.g. AlayaCare scheduler:
# api_filter_property = "start_date_from" (query param), json_watermark_column = "start_at" (payload field)
json_watermark_column = None
# "True" appends $orderby=<prop> asc (OData sources). REQUIRED for watermark
# keyset pagination and safe partial-run watermarks.
incremental_orderby_enabled = "True"
incremental_mode = "True" if api_filter_property else "False"

# -------------------------------------------------------------------------
# PARENT-CHILD (MULTI-LEVEL) CONFIG — optional (e.g. AlayaCare billing)
# Set parent_api_url to enable two-level extraction:
#   parent: https://<tenant>/ext/api/v2/accounting/billing/periods/
#   child : https://<tenant>/ext/api/v2/accounting/billing/periods/{billing_period_id}/invoices
# The notebook pages the parent list, collects the DISTINCT list of parent
# ids (parent_id_column), substitutes each id into EVERY {placeholder} in
# api_url and pages through each child. Only the CHILD records land in
# bronze (with a _parent_id lineage column) — the parent is just a driver.
#
# FULL-LOAD pattern: every run re-reads everything; upserts happen in silver.
# If the API daily limit is hit at ANY point (parent scan or any child page),
# the notebook exits Partial and writes NOTHING — the next run simply starts
# a fresh full load. new_watermark_number is always 0 for these streams.
# max_pages is a HARD stop in this mode: reaching it raises an error rather
# than landing an incomplete full load.
# -------------------------------------------------------------------------
parent_api_url = None
parent_id_column = "id"                  # parent payload field substituted into api_url {placeholder}s
parent_json_data_key = None              # parent record array — defaults to json_data_key

# -------------------------------------------------------------------------
# THREE-LEVEL endpoints — optional extension of parent-child mode.
# Set middle_api_url TOGETHER WITH parent_api_url for chains like:
#   L1: https://<tenant>/ext/api/v2/accounting/billing/periods/
#   L2: https://<tenant>/ext/api/v2/accounting/billing/periods/{billing_period_id}/invoices
#   L3: https://<tenant>/ext/api/v2/accounting/billing/periods/invoice/{invoice_id}/details
# Distinct ids flow down the chain: L1 ids drive L2 calls, distinct L2 ids
# (middle_id_column) drive L3 calls. ONLY the level-3 detail records land in
# bronze (with _parent_id = level-2 id and _grandparent_id = level-1 id).
# Level 3 is a single-object GET (no pagination, no items envelope).
# Same FULL-LOAD, all-or-nothing semantics as parent-child mode: every run
# re-reads everything; a daily limit ANYWHERE → nothing is written.
# -------------------------------------------------------------------------
middle_api_url = None                    # level-2 URL with a {placeholder} for the L1 id
middle_id_column = "id"                  # L2 payload field substituted into api_url's {placeholder}
middle_json_data_key = None              # L2 record array — defaults to json_data_key

# -------------------------------------------------------------------------
# DATE-WINDOW FULL-LOAD CONFIG — optional, for AlayaCare endpoints that
# REQUIRE a bounded date range per call (e.g. forms submissions: max 14 days,
# https://<tenant>/ext/api/v2/tasks/forms20/submissions?submitted_from=...&submitted_to=...)
# The notebook splits window_start_date → window_end_date (default: today,
# UTC) into contiguous windows of window_days and pages through each window.
# FULL-LOAD, all-or-nothing: every run re-reads the whole range (silver
# upserts catch historical updates); a daily limit mid-sweep → NOTHING is
# written and the next run starts a fresh full sweep.
# Window bounds are inclusive (matching the API's >= / <= semantics):
# from = day 00:00:00, to = last day 23:59:59 — e.g. 2026-08-13T00:00:00Z →
# 2026-08-26T23:59:59Z for a 14-day window. All times are UTC.
# -------------------------------------------------------------------------
window_param_from = None            # e.g. "submitted_from"
window_param_to = None              # e.g. "submitted_to"
window_start_date = None            # first day of history, "YYYY-MM-DD" (required in window mode)
window_end_date = None              # optional, "YYYY-MM-DD" — default: today (UTC).
                                    # Stage a big initial load with bounded config rows
                                    # (e.g. 2020→2022, 2022→today) if one sweep exceeds
                                    # the daily quota — each row stays atomic.
window_days = "14"                  # window size in days (the API's max range per call)
window_date_format = "iso_z"        # "iso_z" → 2026-08-13T00:00:00Z (forms) |
                                    # "space_minutes" → 2026-08-13 00:00 (scheduler style)

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Helpers — Key Vault, HTTP session, URL/JSON utilities

# CELL ********************

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, quote

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from notebookutils import mssparkutils


# -------------------------------------------------------------------------
# Defensive coercion — Fabric pipelines pass every parameter as a string
# -------------------------------------------------------------------------
def to_int(value, default=None):
    if value is None or str(value).strip() in ("", "None"):
        return default
    return int(value)


def to_bool(value, default=False):
    if value is None or str(value).strip() in ("", "None"):
        return default
    return str(value).strip().lower() in ("true", "1", "yes")


def to_str(value, default=None):
    if value is None or str(value).strip() in ("", "None"):
        return default
    return str(value).strip()


# -------------------------------------------------------------------------
# Azure Key Vault
# -------------------------------------------------------------------------
def get_key_vault_url(kv):
    """Accepts a vault name or full URL and returns the canonical Key Vault URL."""
    kv = to_str(kv)
    assert kv, "key_vault_name not provided"
    kv = kv.rstrip("/")
    if not kv.startswith("https://"):
        kv = f"https://{kv}.vault.azure.net"
    return kv + "/"


def get_secret(vault_url, secret_name):
    """Reads a secret from Azure Key Vault. Fails fast — a missing secret is a
    configuration failure, not a reason to continue unauthenticated."""
    secret_name = to_str(secret_name)
    assert secret_name, "A Key Vault secret name is required but was not provided."
    try:
        return mssparkutils.credentials.getSecret(vault_url, secret_name)
    except Exception as e:
        raise RuntimeError(
            f"Failed to read secret '{secret_name}' from Key Vault '{vault_url}'. "
            "Verify the secret exists and the identity running this notebook "
            "has 'Get' permission on the vault."
        ) from e


# -------------------------------------------------------------------------
# JSON / URL helpers
# -------------------------------------------------------------------------
def get_nested_value(dic, dot_path):
    """Resolves a value inside a dict. First tries the path as a literal key —
    keys like '@odata.nextLink' contain dots — then falls back to
    dot-notated traversal (e.g. 'data.items')."""
    if not dot_path:
        return None
    if isinstance(dic, dict) and dot_path in dic:
        return dic[dot_path]
    value = dic
    try:
        for key in dot_path.split("."):
            value = value[key]
        return value
    except (KeyError, TypeError, IndexError):
        return None


def upsert_query_param(url, key, value):
    """Adds or replaces a query parameter on a URL (OData-safe: keeps $filter, $top literal)."""
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunparse(parts._replace(query=urlencode(query, safe="$")))


def ensure_query_param(url, key, value):
    """Adds a query parameter only when it is not already present — an explicit
    value in the user-supplied URL always wins."""
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if key in query:
        return url
    return upsert_query_param(url, key, value)


def substitute_path_params(url, record, id_column):
    """Replaces every {placeholder} in a URL with values from a parent record.
    A placeholder first looks up its own name in the record, then falls back to
    id_column — so '{billing_period_id}' and '{id}' both work when
    parent_id_column = 'id'. Fails fast when no value is found."""
    def _replace(match):
        token = match.group(1)
        value = record.get(token, record.get(id_column))
        if value is None:
            raise ValueError(
                f"Cannot substitute '{{{token}}}' in URL '{url}': the parent record has "
                f"neither '{token}' nor the parent_id_column '{id_column}'. "
                "Check parent_id_column against the parent payload."
            )
        return quote(str(value), safe="")
    return re.sub(r"\{([^{}]+)\}", _replace, url)


def dedupe_column_names(raw_columns):
    """Makes flattened column names Spark/Parquet-safe WITHOUT creating
    duplicates. Convention: dots → single underscore (unchanged for all
    existing streams). json_normalize can produce both 'program.client_id'
    (a literal payload key) and 'program.client.id' (a nested path) — the
    single-underscore mapping would merge them, and Parquet/Spark rejects
    duplicate column names. When such a collision family is detected, EVERY
    member of that family is renamed with double underscores so each name
    faithfully encodes its original JSON path:
        program.client_id                     → program__client_id
        program.client.id                     → program__client__id
        program.guarantor_billing_contact_id  → program__guarantor_billing_contact_id
        program.guarantor.billing_contact_id  → program__guarantor__billing_contact_id
        program.guarantor.billing_contact.id  → program__guarantor__billing_contact__id
    Non-colliding columns are untouched; a numeric suffix is the final
    safety net for pathological payloads."""
    cleaned = [c.replace(".", "_").replace(" ", "") for c in raw_columns]
    if len(set(cleaned)) == len(cleaned):
        return cleaned                     # no collisions — existing behaviour
    colliding = {c for c in cleaned if cleaned.count(c) > 1}
    out = []
    for orig, c in zip(raw_columns, cleaned):
        if c in colliding:
            c = orig.replace(".", "__").replace(" ", "")
        out.append(c)
    seen = {}
    final = []
    for c in out:
        if c in seen:
            seen[c] += 1
            final.append(f"{c}_{seen[c]}")
            print(f"WARNING: residual duplicate column '{c}' renamed to '{c}_{seen[c]}'.")
        else:
            seen[c] = 1
            final.append(c)
    print("NOTE: nested JSON paths collided with literal payload keys after flattening "
          "(e.g. 'program.client.id' vs 'program.client_id'). The colliding columns were "
          "disambiguated with double underscores (e.g. 'program__client__id').")
    return final


# -------------------------------------------------------------------------
# Epicor Kinetic REST v2 quirks
# -------------------------------------------------------------------------
_FRACTIONAL_SECONDS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d+")


def strip_fractional_seconds(text):
    """Removes fractional seconds from ISO-8601 datetime literals.
    Epicor's OData parser returns HTTP 500 for filters like
    $filter=PostedDate ge 2023-04-16T03:53:16.2233333Z — second precision
    (2023-04-16T03:53:16Z) works and is sufficient for incremental filtering."""
    if text is None:
        return None
    return _FRACTIONAL_SECONDS_RE.sub(r"\1", str(text))


def format_odata_datetime(value):
    """Normalizes ANY date watermark to the one literal format Epicor reliably
    accepts in $filter: UTC 'yyyy-MM-ddTHH:mm:ssZ'.

    Epicor's OData parser returns HTTP 500 for:
    * fractional seconds          (2023-04-11T21:23:44.087+10:00)
    * explicit timezone offsets   (2023-04-11T21:23:44+10:00) — the format Epicor
      itself returns in payloads, e.g. PostedDate in tenant local time

    Handles payload values (offset-aware), ISO 'Z' values, and naive SQL-style
    config-table values ('2023-04-11 11:23:44.0866667'). Naive values are assumed
    to be UTC — matching how the pipeline persists watermarks to the config table.
    Fractional seconds are truncated; with 'ge' the overlap is deduplicated."""
    ts = pd.Timestamp(str(value))
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_date_watermark(value):
    """format_odata_datetime with a safe fallback for unparseable values."""
    try:
        return format_odata_datetime(value)
    except (ValueError, TypeError):
        return strip_fractional_seconds(value)


def sanitize_api_url(url):
    """Fixes known Epicor Kinetic issues in a user-supplied URL:
    * $top + $count conflict — Epicor silently caps results at 100 when both
      are present, so $count is removed whenever $top is used.
    * Fractional seconds in $filter datetime literals → HTTP 500."""
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))

    if "$top" in query and "$count" in query:
        print("WARNING: $top and $count combined — Epicor ignores $top in this "
              "combination (caps at 100 records). $count was removed from the URL. "
              "Pagination is handled by the watermark keyset fallback instead.")
        del query["$count"]

    if "$filter" in query:
        sanitized = strip_fractional_seconds(query["$filter"])
        if sanitized != query["$filter"]:
            print("Fractional seconds stripped from $filter datetime literal(s) "
                  "(Epicor returns HTTP 500 for sub-second precision).")
            query["$filter"] = sanitized

    return urlunparse(parts._replace(query=urlencode(query, safe="$()")))


def add_incremental_filter(url, style, filter_property, watermark, operator="ge", delta_format="Date"):
    """Applies the watermark clause to the URL in the source system's style:
    * odata_filter → $filter=<prop> <operator> <watermark> (combined with any existing $filter)
    * query_param  → <prop>=<watermark> (e.g. AlayaCare start_date_from)
    Fractional seconds are stripped for Date watermarks (Epicor HTTP 500 quirk)."""
    if style == "odata_filter":
        parts = urlparse(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        # Date watermarks are normalised to UTC 'yyyy-MM-ddTHH:mm:ssZ' — the only
        # literal format Epicor reliably accepts (500s on fractions and offsets)
        wm = normalize_date_watermark(watermark) if delta_format == "Date" else watermark
        new_clause = f"{filter_property} {operator} {wm}"
        existing = query.get("$filter")
        query["$filter"] = f"({existing}) and ({new_clause})" if existing else new_clause
        return urlunparse(parts._replace(query=urlencode(query, safe="$()")))
    # query_param — Date watermarks are sent in the source's documented format
    # (AlayaCare: 'YYYY-MM-DD HH:mm', UTC by default) regardless of how the
    # watermark was stored (ISO 'Z', offset-aware, or SQL-style)
    if delta_format == "Date":
        try:
            ts = pd.Timestamp(str(watermark))
            ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
            watermark = ts.strftime("%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            pass
    return upsert_query_param(url, filter_property, watermark)


def add_orderby(url, filter_property):
    """Appends $orderby=<property> asc (unless the URL already has $orderby).
    Ascending order is required for watermark keyset pagination and makes a
    partial run's watermark a safe resume point."""
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if "$orderby" not in query:
        query["$orderby"] = f"{filter_property} asc"
    return urlunparse(parts._replace(query=urlencode(query, safe="$(),")))


def is_api_limit(response, limit_codes, limit_substring):
    """True when the response means 'API daily limit reached' rather than a
    regular error. Checks the status code, and optionally an error-body
    substring for APIs that signal quota exhaustion with 400/500."""
    if response.status_code in limit_codes:
        return True
    if response.status_code >= 400 and limit_substring and limit_substring in response.text:
        return True
    return False


# -------------------------------------------------------------------------
# Watermark keyset helpers (Epicor fallback pagination)
# -------------------------------------------------------------------------
def record_hash(record):
    """Stable hash of a raw record — used to drop boundary duplicates when the
    keyset fallback re-fetches the overlap (ge operator on Date columns)."""
    return hashlib.sha1(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def parse_watermark_value(value, delta_format):
    """Parses a watermark value for TYPE-CORRECT comparison:
    * Number → float (string max would rank '9' above '1000')
    * Date   → timezone-AWARE pandas Timestamp. Naive values are localised to UTC
      (the config-table convention) so comparisons against offset-aware payload
      values ('+10:00') never degrade to string ordering — aware-vs-naive
      comparisons raise TypeError, which silently falls back to unreliable
      string comparison of differently formatted timestamps."""
    if delta_format == "Number":
        return float(value)
    try:
        ts = pd.Timestamp(str(value))
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts
    except Exception:
        return str(value)


def wm_gt(a, b):
    """a > b with a string-compare fallback for unparseable values."""
    try:
        return a > b
    except TypeError:
        return str(a) > str(b)


def page_watermark_minmax(records, filter_property, delta_format):
    """Returns (min_raw, min_parsed, max_raw, max_parsed) of filter_property
    across a page of raw records. Raw string is kept for filter clauses and
    output; parsed value is used for comparisons."""
    min_raw = min_parsed = max_raw = max_parsed = None
    for r in records:
        v = get_nested_value(r, filter_property)
        if v is None:
            continue
        raw = str(v)
        try:
            parsed = parse_watermark_value(raw, delta_format)
        except (ValueError, TypeError):
            parsed = raw
        if min_raw is None or wm_gt(min_parsed, parsed):
            min_raw, min_parsed = raw, parsed
        if max_raw is None or wm_gt(parsed, max_parsed):
            max_raw, max_parsed = raw, parsed
    return min_raw, min_parsed, max_raw, max_parsed


# -------------------------------------------------------------------------
# HTTP session with automatic retry / back-off
# -------------------------------------------------------------------------
def build_http_session(retries):
    """Retries 429 (throttling) and 5xx with exponential back-off,
    honouring the Retry-After header."""
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Assertions — config, credentials, incremental setup

# CELL ********************

# -------------------------------------------------------------------------
# Validate & normalise configuration
# -------------------------------------------------------------------------
assert to_str(api_url), "api_url not provided"
assert to_str(destination_raw_file_system), "destination_raw_file_system not provided"
assert to_str(destination_raw_file_folder), "destination_raw_file_folder not provided"
assert to_str(destination_raw_file), "destination_raw_file not provided"
assert to_str(username_secret_name), "username_secret_name not provided"
assert to_str(password_secret_name), "password_secret_name not provided"
assert to_str(pagination_type), "pagination_type not provided ('next_url', 'page_number' or 'none')"

pagination_type = to_str(pagination_type).lower()
assert pagination_type in ("next_url", "page_number", "none"), \
    "pagination_type must be 'next_url', 'page_number' or 'none'"
if pagination_type == "next_url":
    assert to_str(pagination_next_url_key), \
        "pagination_next_url_key is required when pagination_type = 'next_url'"

# -------------------------------------------------------------------------
# Parent-child (multi-level) mode — e.g. AlayaCare billing periods → invoices.
# FULL-LOAD pattern: no watermark state of any kind — every run re-reads
# everything and the run is atomic (limit hit anywhere → nothing is written).
# -------------------------------------------------------------------------
parent_mode = to_str(parent_api_url) is not None
if parent_mode:
    assert re.search(r"\{[^{}]+\}", to_str(api_url)), \
        "parent-child mode: api_url must contain a {placeholder}, e.g. " \
        "'.../billing/periods/{billing_period_id}/invoices'"
    assert pagination_type == "page_number", \
        "parent-child mode currently supports pagination_type = 'page_number' only (AlayaCare)"
    parent_id_column = to_str(parent_id_column, "id")
    parent_json_data_key = to_str(parent_json_data_key) or to_str(json_data_key)
    assert to_str(parent_json_data_key), \
        "parent-child mode: json_data_key (or parent_json_data_key) is required"
    parent_api_url = sanitize_api_url(parent_api_url)
    if to_str(api_filter_property):
        print("NOTE: parent-child mode — full-load pattern; api_filter_property / "
              "incremental filters and last_watermark are ignored.")
    print(f"Parent-child mode ON (full load). Parent: {parent_api_url}")

# Three-level mode (L1 → L2 → L3) — e.g. billing periods → invoices → invoice details
three_level_mode = to_str(middle_api_url) is not None
if three_level_mode:
    assert parent_mode, \
        "three-level mode: parent_api_url (level 1) is required together with middle_api_url (level 2)"
    assert re.search(r"\{[^{}]+\}", to_str(middle_api_url)), \
        "three-level mode: middle_api_url must contain a {placeholder} for the level-1 id, e.g. " \
        "'.../billing/periods/{billing_period_id}/invoices'"
    middle_id_column = to_str(middle_id_column, "id")
    middle_json_data_key = to_str(middle_json_data_key) or to_str(json_data_key)
    middle_api_url = sanitize_api_url(middle_api_url)
    print(f"Three-level mode ON (full load). L2: {middle_api_url} → L3: {api_url}")

# -------------------------------------------------------------------------
# Date-window full-load mode — endpoints that REQUIRE a bounded date range
# per call (e.g. AlayaCare forms submissions: max 14 days). No watermark
# state: every run re-reads the whole range; all-or-nothing on limit.
# -------------------------------------------------------------------------
window_mode = to_str(window_param_from) is not None or to_str(window_param_to) is not None
windows = []
if window_mode:
    assert to_str(window_param_from) and to_str(window_param_to), \
        "window mode: both window_param_from and window_param_to are required"
    assert not parent_mode, \
        "window mode cannot be combined with parent-child / three-level mode"
    window_days = to_int(window_days, 14)
    assert 1 <= window_days <= 31, "window_days must be between 1 and 31"
    window_date_format = to_str(window_date_format, "iso_z").lower()
    assert window_date_format in ("iso_z", "space_minutes"), \
        "window_date_format must be 'iso_z' or 'space_minutes'"
    assert to_str(window_start_date), \
        "window mode: window_start_date is required (first day of history, 'YYYY-MM-DD')"
    try:
        w_start = datetime.strptime(to_str(window_start_date), "%Y-%m-%d").date()
        w_end = (datetime.strptime(to_str(window_end_date), "%Y-%m-%d").date()
                 if to_str(window_end_date) else datetime.now(timezone.utc).date())
    except ValueError:
        raise AssertionError("window_start_date / window_end_date must be 'YYYY-MM-DD'")
    # plain comparisons instead of min()/max() — `%run /commonTransforms` does
    # `from pyspark.sql.functions import *`, which shadows the builtins with the
    # single-argument Spark aggregates (TypeError: min() takes 1 positional arg)
    w_today = datetime.now(timezone.utc).date()
    w_end = w_end if w_end < w_today else w_today
    assert w_start <= w_end, "window_start_date must not be after window_end_date"
    fmt = "%Y-%m-%dT%H:%M:%SZ" if window_date_format == "iso_z" else "%Y-%m-%d %H:%M"
    day = w_start
    while day <= w_end:
        last = day + timedelta(days=window_days - 1)
        if last > w_end:
            last = w_end
        windows.append((
            datetime(day.year, day.month, day.day, 0, 0, 0).strftime(fmt),
            datetime(last.year, last.month, last.day, 23, 59, 59).strftime(fmt),
        ))
        day = last + timedelta(days=1)
    if to_str(api_filter_property):
        print("NOTE: window mode — api_filter_property / incremental filters and "
              "last_watermark are ignored; the date range is driven by the windows.")
    print(f"Window mode ON (full load): {len(windows)} window(s) of up to {window_days} "
          f"day(s), {windows[0][0]} → {windows[-1][1]} (format: {window_date_format})")

# Default start for the very first run
default_date = "2000-01-01T00:00:00Z"
default_start_number = 0

max_pages = to_int(max_pages, 100)
request_timeout_seconds = to_int(request_timeout_seconds, 60)
max_retries = to_int(max_retries, 5)
page_size = to_int(page_size, 100)
write_raw_json = to_bool(write_raw_json, True)
incremental_mode = to_bool(incremental_mode, False)
incremental_orderby_enabled = to_bool(incremental_orderby_enabled, True)
delta_format = to_str(delta_format)

if incremental_mode:
    assert delta_format in ("Date", "Number"), \
        "delta_format must be 'Date' or 'Number' when incremental mode is on"
    initial_watermark_default = default_date if delta_format == "Date" else default_start_number
else:
    initial_watermark_default = None
    print("Incremental Mode is Off")

if pagination_type == "next_url" and not to_str(api_filter_property):
    print("WARNING: api_filter_property is not set. If the API omits @odata.nextLink "
          "on a full page (Epicor caps at 100 and ignores $skip), the run will FAIL. "
          "Set api_filter_property + delta_format to enable watermark keyset pagination.")

# Rate-limit (daily quota) configuration
limit_codes = {int(c.strip()) for c in str(api_limit_http_codes).split(",") if c.strip()}
limit_substring = to_str(api_limit_error_substring)
partial_run_unordered_policy = to_str(partial_run_unordered_policy, "hold_watermark").lower()
assert partial_run_unordered_policy in ("hold_watermark", "max_fetched"), \
    "partial_run_unordered_policy must be 'hold_watermark' or 'max_fetched'"

# A partial (limit-cut) run may only advance the watermark to the max fetched value
# when rows are guaranteed to arrive in ascending order — i.e. OData sources with
# the auto-appended $orderby. AlayaCare documents no sort parameter, so it is
# treated as unordered and governed by partial_run_unordered_policy.
incremental_style = to_str(incremental_filter_style, "odata_filter").lower()
ordering_guaranteed = incremental_mode and incremental_style == "odata_filter" and incremental_orderby_enabled

# Payload column carrying the watermark value — usually identical to the API-side
# filter property (Epicor), but not always (AlayaCare: param start_date_from vs
# payload field start_at)
watermark_column = to_str(json_watermark_column) or to_str(api_filter_property)

# Fix known Epicor quirks in the user-supplied URL
# ($top+$count conflict, fractional seconds in $filter)
api_url = sanitize_api_url(api_url)

# -------------------------------------------------------------------------
# Authentication — Basic Auth from Key Vault; X-API-Key only when configured
# -------------------------------------------------------------------------
kv_url = get_key_vault_url(key_vault_name)
api_username = get_secret(kv_url, username_secret_name)
api_password = get_secret(kv_url, password_secret_name)

api_auth = (api_username, api_password)   # requests sends this as Basic Auth
api_headers = {
    "Accept": "application/json",
    "User-Agent": "Fabric-Notebook-API-Ingestion",
}
if to_str(api_key_secret_name):
    api_headers["X-API-Key"] = get_secret(kv_url, api_key_secret_name)
    print("Auth: Basic Auth + X-API-Key header")
else:
    print("Auth: Basic Auth only")

session = build_http_session(max_retries)
batch_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")

# -------------------------------------------------------------------------
# Incremental / keyset setup
# keyset_base_url never carries the auto-added watermark clause — each keyset
# request rebuilds the URL with the current watermark value, so the $filter
# stays short no matter how many pages are fetched.
# -------------------------------------------------------------------------
keyset_base_url = api_url
global_max_watermark_found = None      # raw string (for output)
global_max_watermark_parsed = None     # parsed (for typed comparison)
current_url = api_url

if incremental_mode and not parent_mode and not window_mode:
    effective_watermark = to_str(last_watermark) or to_str(initial_watermark_default)
    assert effective_watermark is not None and str(effective_watermark) != "", (
        "incremental_mode is on but neither last_watermark nor "
        "initial_watermark_default was provided"
    )
    assert to_str(api_filter_property), "api_filter_property is required for incremental mode"
    assert incremental_style in ("odata_filter", "query_param"), \
        "incremental_filter_style must be 'odata_filter' or 'query_param'"
    effective_watermark = str(effective_watermark)

    # Ascending order is REQUIRED for keyset pagination + safe partial-run resume
    if incremental_style == "odata_filter" and incremental_orderby_enabled:
        keyset_base_url = add_orderby(keyset_base_url, api_filter_property)
    if not ordering_guaranteed:
        print(f"NOTE: this source has no guaranteed date ordering — if the run is cut short by the "
              f"API daily limit, watermark policy '{partial_run_unordered_policy}' applies.")

    # Initial operator: Date resumes with ge (boundary overlap deduped),
    # Number resumes with gt (unique monotonic column — exact resume)
    key_operator = "ge" if delta_format == "Date" else "gt"
    print(f"Incremental Mode ON ({incremental_style}). Fetching records where "
          f"{api_filter_property} {key_operator} {effective_watermark}")
    current_url = add_incremental_filter(
        keyset_base_url, incremental_style, api_filter_property,
        effective_watermark, operator=key_operator, delta_format=delta_format,
    )
    global_max_watermark_found = effective_watermark
    global_max_watermark_parsed = parse_watermark_value(effective_watermark, delta_format)

# Keyset pagination state (Epicor full-page fallback)
keyset_active = False
key_value_raw = global_max_watermark_found
key_value_parsed = global_max_watermark_parsed
seen_hashes = set()

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Main Execution — extraction loop, bronze write, watermark

# CELL ********************

print(f"Starting extraction from: {parent_api_url if parent_mode else current_url}")


def extract_all_pages(start_url, keyset_base, initial_max_wm_raw, initial_max_wm_parsed,
                      watermark_col, data_key=None, label="", pagination_style=None,
                      raw_response=False):
    """Pages through ONE endpoint until exhaustion, short page, HTTP 404 or the
    API daily limit. Shared by single-level streams and every level of the
    parent-child / three-level chains. pagination_style overrides the global
    pagination_type for this call (e.g. 'none' for a single-object level-3
    GET); raw_response=True treats the whole response body as the record (no
    json_data_key envelope). Returns a dict with the records, the raw page
    payloads, the page count, the limit flag and the highest watermark seen."""
    prefix = f"[{label}] " if label else ""
    pag = pagination_style or pagination_type
    data_key = None if raw_response else (data_key or json_data_key)
    page_count = 0
    limit_hit = False
    records_out = []
    raw_pages_out = []
    max_wm_raw = initial_max_wm_raw
    max_wm_parsed = initial_max_wm_parsed
    current = start_url
    base = start_url                # first-request URL (watermark filter applied)
    observed_page_size = None       # set from the first page = the server's page size
    # Keyset pagination state (Epicor full-page fallback)
    keyset_on = False
    key_raw = initial_max_wm_raw
    key_parsed = initial_max_wm_parsed
    key_op = "ge" if delta_format == "Date" else "gt"
    seen = set()

    # ------------------------------------------------------------------
    # Extraction loop
    #   next_url    → follows @odata.nextLink when present; on a FULL page
    #                 without a nextLink (Epicor caps at 100, ignores $skip),
    #                 falls back to watermark keyset pagination:
    #                 $filter=<prop> ge/gt <max value of the batch>, repeated
    #                 until an empty or short page
    #   page_number → requests ?page=N&count=M, stops at total_pages (AlayaCare)
    # ------------------------------------------------------------------
    while current and page_count < max_pages:
        page_count += 1

        # Page-number pagination is expressed as query params on the base URL
        if pag == "page_number":
            request_url = upsert_query_param(base, page_number_param, page_count)
            if to_str(page_size_param) and page_size:
                request_url = upsert_query_param(request_url, page_size_param, page_size)
        else:
            request_url = current

        print(f"{prefix}Processing page {page_count}")
        response = session.get(
            request_url,
            headers=api_headers,
            auth=api_auth,
            timeout=request_timeout_seconds,
        )

        # API daily limit — stop gracefully, keep what we have.
        # (Transient 429s are already absorbed by the session's retry/back-off;
        #  reaching this point means retries were exhausted → hard quota stop.)
        if is_api_limit(response, limit_codes, limit_substring):
            limit_hit = True
            page_count -= 1   # this page was never fetched
            print(f"{prefix}API daily limit reached (HTTP {response.status_code}) after {page_count} page(s). "
                  "Stopping gracefully — writing records fetched so far. "
                  "Next run will resume from the returned high watermark.")
            break

        if response.status_code == 404:
            print(f"{prefix}HTTP 404 — no more data. Stopping pagination.")
            break
        if response.status_code == 401:
            raise PermissionError(
                "HTTP 401 Unauthorized — check the username/password secrets in Key Vault."
            )
        if response.status_code == 403:
            raise PermissionError(
                "HTTP 403 Forbidden — the API key is missing/invalid or the account lacks "
                "access to this service. Check the X-API-Key secret (if used) in Key Vault."
            )
        response.raise_for_status()

        data = response.json()
        raw_pages_out.append(data)

        # Record array — "value" (Epicor), "items" (AlayaCare), or the raw
        # body itself (single-object endpoints such as invoice details)
        records = data if raw_response else (get_nested_value(data, data_key) if to_str(data_key) else data)
        if isinstance(records, dict):
            records = [records]
        if records is None:
            raise ValueError(
                f"json_data_key '{data_key}' was not found in the API response. "
                "Check the metadata configuration for this stream."
            )
        if len(records) == 0:
            print(f"{prefix}No records on this page — extraction complete.")
            break

        raw_count = len(records)                # server page size, before dedupe
        if observed_page_size is None:
            observed_page_size = raw_count      # first page reveals the server's page size

        # Typed min/max of the watermark property on this page (raw records)
        page_min_raw = page_min_parsed = page_max_raw = page_max_parsed = None
        if watermark_col:
            page_min_raw, page_min_parsed, page_max_raw, page_max_parsed = \
                page_watermark_minmax(records, watermark_col, delta_format)
            # $orderby sanity check — keyset pagination is unsafe if the server
            # does not actually return rows in ascending watermark order
            if (
                keyset_on
                and page_min_parsed is not None
                and key_parsed is not None
                and wm_gt(key_parsed, page_min_parsed)
            ):
                print(f"{prefix}WARNING: page contains {api_filter_property} values below the current "
                      f"keyset watermark ({page_min_raw} < {key_raw}). The server may not "
                      "be honouring $orderby — keyset pagination and partial-run resume are "
                      "only safe on ordered data. Verify ordering on this endpoint.")

        # Drop boundary duplicates re-fetched by the keyset 'ge' overlap
        if keyset_on:
            new_records = []
            for r in records:
                h = record_hash(r)
                if h not in seen:
                    seen.add(h)
                    new_records.append(r)
            dup_count = len(records) - len(new_records)
            if dup_count:
                print(f"{prefix}Keyset overlap: dropped {dup_count} boundary duplicate record(s).")
            records = new_records
        else:
            for r in records:
                seen.add(record_hash(r))

        records_out.extend(records)
        new_count = len(records)
        if page_max_raw is not None and (
            max_wm_parsed is None or wm_gt(page_max_parsed, max_wm_parsed)
        ):
            max_wm_raw = page_max_raw
            max_wm_parsed = page_max_parsed

        print(f"{prefix}Page {page_count}: {new_count} records (running total: {len(records_out)})")

        # --------------------------------------------------------------
        # Next-page resolution
        # --------------------------------------------------------------
        if pag == "next_url":
            next_link = get_nested_value(data, pagination_next_url_key)
            raw_page_full = raw_count >= observed_page_size
            if next_link and next_link != current:
                current = next_link
            elif raw_page_full:
                # Full page without a nextLink — Epicor ignores $skip, so walk
                # forward on the watermark property instead (keyset pagination).
                if not to_str(api_filter_property):
                    raise RuntimeError(
                        "The API returned a full page without @odata.nextLink, and this "
                        "tenant does not honour $skip. Cannot paginate further: set "
                        "api_filter_property (and delta_format) for this stream to enable "
                        "watermark keyset pagination, or narrow the result set with $filter."
                    )
                keyset_on = True
                if page_max_raw is not None and wm_gt(page_max_parsed, key_parsed):
                    key_raw, key_parsed = page_max_raw, page_max_parsed
                elif new_count == 0:
                    # Boundary tie stall: a full page yielded only records already
                    # seen at the current watermark value. ge cannot advance past
                    # >= page-size identical values — escalate this step to gt.
                    if key_op == "ge":
                        key_op = "gt"
                        print(f"{prefix}WARNING: tie stall at {api_filter_property} = {key_raw} "
                              "(a full page of boundary duplicates). Escalating to 'gt' for the "
                              "next request — if more records share this exact value they will "
                              "be skipped. For tie-heavy streams use a unique Number property "
                              "(e.g. SysRevID) as api_filter_property.")
                    else:
                        print(f"{prefix}No forward progress even with 'gt' — stopping pagination.")
                        current = None
                if current is not None:
                    current = add_incremental_filter(
                        keyset_base, incremental_style, api_filter_property,
                        key_raw, operator=key_op, delta_format=delta_format,
                    )
                    print(f"{prefix}Watermark keyset fallback: {api_filter_property} {key_op} {key_raw}")
            else:
                current = None   # short page = last page
        elif pag == "page_number":
            total_pages = to_int(get_nested_value(data, total_pages_key), None)
            if total_pages is not None and page_count >= total_pages:
                current = None                      # reached the last declared page
            elif page_size and len(records) < page_size:
                current = None                      # short page = last page
            # else: loop continues; page_count increments
        else:  # "none"
            current = None

    if page_count >= max_pages and not limit_hit:
        print(f"{prefix}WARNING: reached max_pages safety stop ({max_pages}). "
              "Data may be incomplete — review pagination configuration.")

    print(f"{prefix}Extraction finished. Records collected: {len(records_out)}")
    return {
        "records": records_out,
        "raw_pages": raw_pages_out,
        "page_count": page_count,
        "limit_hit": limit_hit,
        "max_pages_hit": page_count >= max_pages and not limit_hit,
        "max_watermark_raw": max_wm_raw,
        "max_watermark_parsed": max_wm_parsed,
    }


page_count = 0
total_records = 0
total_records_written = 0
final_watermark = ""
limit_hit = False
all_records = []
raw_pages = []

try:
    if three_level_mode:
        # ==================================================================
        # THREE-LEVEL MODE — e.g. billing periods → invoices → invoice details
        # FULL-LOAD, all-or-nothing: every run re-reads everything (silver
        # upserts); a daily limit at ANY level → NOTHING is written and the
        # next run starts a fresh full load.
        # Phase 1: distinct level-1 ids (parent scan)
        # Phase 2: level-2 per level-1 id → distinct level-2 ids (+ lineage)
        # Phase 3: level-3 detail GET per level-2 id (single object)
        # Phase 4: land ONLY the level-3 records
        # ==================================================================

        def exit_partial_limit(pages_read, rows_read, parents_found, parents_completed,
                               invoices_found=0):
            exit_values = {
                "status": "Partial",
                "limit_reached": True,
                "batch_id": batch_id,
                "pages_read": pages_read,
                "rows_read": rows_read,
                "rows_copied": 0,
                "parents_found": parents_found,
                "parents_completed": parents_completed,
                "invoices_found": invoices_found,
                "new_watermark_date": "1900-01-01",
                "new_watermark_number": 0,
            }
            exit_json = json.dumps(exit_values)
            print(f"Exiting with: {exit_json}")
            mssparkutils.notebook.exit(exit_json)

        # ---------------- Phase 1: distinct level-1 ids ----------------
        parent_res = extract_all_pages(
            parent_api_url, parent_api_url, None, None, None,
            data_key=parent_json_data_key, label="L1 parent scan",
        )
        if parent_res["max_pages_hit"]:
            raise RuntimeError(
                f"Level-1 scan reached the max_pages safety stop ({max_pages}). The id "
                "list would be incomplete — nothing was written. Increase max_pages "
                "for this stream.")
        if parent_res["limit_hit"]:
            print("API daily limit hit while reading level 1 — exiting WITHOUT writing "
                  "anything. The next run starts a fresh full load.")
            exit_partial_limit(parent_res["page_count"], 0, 0, 0)

        seen_parent_ids = set()
        parent_ids = []
        for r in parent_res["records"]:
            pid = r.get(parent_id_column)
            if pid is None:
                raise ValueError(
                    f"A level-1 record is missing parent_id_column '{parent_id_column}': "
                    f"{json.dumps(r, default=str)[:200]}. Check parent_id_column "
                    "against the level-1 payload.")
            if pid not in seen_parent_ids:
                seen_parent_ids.add(pid)
                parent_ids.append(pid)
        print(f"Level 1 complete: {len(parent_ids)} distinct id(s) in "
              f"{parent_res['page_count']} page(s).")

        # ---------------- Phase 2: distinct level-2 ids (with L1 lineage) ----------------
        seen_middle_ids = set()
        middle_pairs = []                    # (level2_id, level1_id), first-seen order
        l2_page_total = 0
        l1_completed = 0
        for idx, pid in enumerate(parent_ids, 1):
            middle_url = substitute_path_params(middle_api_url, {parent_id_column: pid}, parent_id_column)
            print(f"--- L1 {parent_id_column}={pid} ({idx}/{len(parent_ids)}): {middle_url}")
            middle_res = extract_all_pages(
                middle_url, middle_url, None, None, None,
                data_key=middle_json_data_key, label=f"L2 of {pid}",
            )
            l2_page_total += middle_res["page_count"]
            if middle_res["max_pages_hit"]:
                raise RuntimeError(
                    f"Level-2 extraction for {parent_id_column}={pid} reached the max_pages "
                    f"safety stop ({max_pages}). Data would be incomplete — nothing was "
                    "written. Increase max_pages for this stream.")
            if middle_res["limit_hit"]:
                print(f"API daily limit hit while reading level 2 of {parent_id_column}={pid} "
                      "— exiting WITHOUT writing anything. The next run starts a fresh "
                      "full load.")
                exit_partial_limit(parent_res["page_count"] + l2_page_total, 0,
                                   len(parent_ids), l1_completed)
            for r in middle_res["records"]:
                mid = r.get(middle_id_column)
                if mid is None:
                    raise ValueError(
                        f"A level-2 record is missing middle_id_column '{middle_id_column}': "
                        f"{json.dumps(r, default=str)[:200]}. Check middle_id_column "
                        "against the level-2 payload.")
                if mid not in seen_middle_ids:
                    seen_middle_ids.add(mid)
                    middle_pairs.append((mid, pid))
            l1_completed += 1
        print(f"Level 2 complete: {len(middle_pairs)} distinct id(s) across "
              f"{l1_completed} parent(s).")

        # ---------------- Phase 3: level-3 details ----------------
        detail_records = []
        detail_raw_pages = []
        l3_page_total = 0
        l2_completed = 0
        for idx, (mid, pid) in enumerate(middle_pairs, 1):
            detail_url = substitute_path_params(api_url, {middle_id_column: mid}, middle_id_column)
            print(f"--- L3 for {middle_id_column}={mid} ({idx}/{len(middle_pairs)}): {detail_url}")
            detail_res = extract_all_pages(
                detail_url, detail_url, None, None, None,
                raw_response=True, pagination_style="none", label=f"L3 of {mid}",
            )
            l3_page_total += detail_res["page_count"]
            if detail_res["limit_hit"]:
                print(f"API daily limit hit while reading level 3 of {middle_id_column}={mid} "
                      "— exiting WITHOUT writing anything. The next run starts a fresh "
                      "full load.")
                exit_partial_limit(parent_res["page_count"] + l2_page_total + l3_page_total,
                                   len(detail_records), len(parent_ids), l1_completed,
                                   invoices_found=len(middle_pairs))
            for rec in detail_res["records"]:
                rec["_parent_id"] = mid            # level-2 id (invoice)
                rec["_grandparent_id"] = pid       # level-1 id (billing period)
            detail_records.extend(detail_res["records"])
            detail_raw_pages.extend(
                {"_parent_id": mid, "_grandparent_id": pid, "payload": page}
                for page in detail_res["raw_pages"]
            )
            l2_completed += 1

        total_records = len(detail_records)
        print(f"Level 3 complete: {l2_completed}/{len(middle_pairs)} detail(s) fetched — "
              f"{total_records} record(s) collected.")

        # ---------------- Phase 4: bronze write (complete runs only) ----------------
        if total_records > 0:
            print("Flattening level-3 data...")
            pdf = pd.json_normalize(detail_records).astype(str)
            pdf.columns = dedupe_column_names(list(pdf.columns))
            pdf["_ingestion_batch_id"] = batch_id
            pdf["_ingestion_ts_utc"] = datetime.now(timezone.utc).isoformat()
            pdf["_source_url"] = api_url
            total_records_written = len(pdf)
            writeFilePandas(
                pdf,
                "bronze",
                destination_raw_file_system,
                destination_raw_file_folder,
                destination_raw_file,
            )
            if write_raw_json:
                destination_raw_json_file_folder = destination_raw_file_folder.replace("raw_bronze", "raw_json_bronze")
                destination_raw_json_file = destination_raw_file.replace("parquet", "json")
                raw_path = (
                    f"{getAbfsPath('bronze')}/{destination_raw_file_system}/"
                    f"{destination_raw_json_file_folder}/{destination_raw_json_file}"
                )
                raw_content = "\n".join(json.dumps(page, default=str) for page in detail_raw_pages)
                mssparkutils.fs.put(raw_path, raw_content, True)
                print(f"Raw payload written to: {raw_path}")
        else:
            print("No level-3 records extracted — nothing written for this stream.")

        run_status = "Success"
        print(f"{run_status}. Level-3 records: {total_records}, "
              f"L2 ids: {l2_completed}/{len(middle_pairs)}, L1 ids: {l1_completed}/{len(parent_ids)}")
        exit_values = {
            "status": run_status,
            "limit_reached": False,
            "batch_id": batch_id,
            "pages_read": parent_res["page_count"] + l2_page_total + l3_page_total,
            "rows_read": total_records,
            "rows_copied": total_records_written,
            "parents_found": len(parent_ids),
            "parents_completed": l1_completed,
            "invoices_found": len(middle_pairs),
            "new_watermark_date": "1900-01-01",
            "new_watermark_number": 0,
        }
        exit_json = json.dumps(exit_values)
        print(f"Exiting with: {exit_json}")
        mssparkutils.notebook.exit(exit_json)

    elif parent_mode:
        # ==================================================================
        # PARENT-CHILD MODE — e.g. AlayaCare billing periods → invoices
        # FULL-LOAD pattern: every run re-reads everything; silver upserts.
        # Phase 1: page the parent list, collect the DISTINCT parent ids
        # Phase 2: substitute each id into api_url's {placeholder} and page
        #          through the child endpoint
        # Phase 3: land the child records — ONLY on a complete run. If the
        #          daily limit is hit anywhere (parent or child), NOTHING is
        #          written and the next run starts a fresh full load.
        # ==================================================================

        # ---------------- Phase 1: distinct parent ids ----------------
        parent_res = extract_all_pages(
            parent_api_url, parent_api_url, None, None, None,
            data_key=parent_json_data_key, label="parent scan",
        )
        if parent_res["max_pages_hit"]:
            raise RuntimeError(
                f"Parent scan reached the max_pages safety stop ({max_pages}). The parent "
                "id list would be incomplete — nothing was written. Increase max_pages "
                "for this stream.")
        if parent_res["limit_hit"]:
            print("API daily limit hit while reading the parent list — exiting WITHOUT "
                  "writing anything. The next run starts a fresh full load.")
            exit_values = {
                "status": "Partial",
                "limit_reached": True,
                "batch_id": batch_id,
                "pages_read": parent_res["page_count"],
                "rows_read": 0,
                "rows_copied": 0,
                "parents_found": 0,
                "parents_completed": 0,
                "new_watermark_date": "1900-01-01",
                "new_watermark_number": 0,
            }
            exit_json = json.dumps(exit_values)
            print(f"Exiting with: {exit_json}")
            mssparkutils.notebook.exit(exit_json)

        # Distinct parent ids, first-seen order
        seen_parent_ids = set()
        parent_ids = []
        for r in parent_res["records"]:
            pid = r.get(parent_id_column)
            if pid is None:
                raise ValueError(
                    f"A parent record is missing parent_id_column '{parent_id_column}': "
                    f"{json.dumps(r, default=str)[:200]}. Check parent_id_column "
                    "against the parent payload.")
            if pid not in seen_parent_ids:
                seen_parent_ids.add(pid)
                parent_ids.append(pid)
        print(f"Parent scan complete: {len(parent_ids)} distinct parent id(s) in "
              f"{parent_res['page_count']} page(s).")

        # ---------------- Phase 2: child extraction per parent id ----------------
        child_records = []
        child_raw_pages = []
        child_page_total = 0
        parents_completed = 0
        for idx, pid in enumerate(parent_ids, 1):
            child_url = substitute_path_params(api_url, {parent_id_column: pid}, parent_id_column)
            print(f"--- Parent {parent_id_column}={pid} ({idx}/{len(parent_ids)}): {child_url}")
            child_res = extract_all_pages(
                child_url, child_url, None, None, None,
                label=f"parent {pid}",
            )
            child_page_total += child_res["page_count"]
            if child_res["max_pages_hit"]:
                raise RuntimeError(
                    f"Child extraction for parent {pid} reached the max_pages safety stop "
                    f"({max_pages}). Data would be incomplete — nothing was written. "
                    "Increase max_pages for this stream.")
            if child_res["limit_hit"]:
                # All-or-nothing: a partial child set must NOT land — the next
                # run starts a fresh full load.
                print(f"API daily limit hit while reading children of parent {pid} — "
                      "exiting WITHOUT writing anything. The next run starts a fresh "
                      "full load.")
                exit_values = {
                    "status": "Partial",
                    "limit_reached": True,
                    "batch_id": batch_id,
                    "pages_read": parent_res["page_count"] + child_page_total,
                    "rows_read": len(child_records),
                    "rows_copied": 0,
                    "parents_found": len(parent_ids),
                    "parents_completed": parents_completed,
                    "new_watermark_date": "1900-01-01",
                    "new_watermark_number": 0,
                }
                exit_json = json.dumps(exit_values)
                print(f"Exiting with: {exit_json}")
                mssparkutils.notebook.exit(exit_json)
            for rec in child_res["records"]:
                rec["_parent_id"] = pid              # lineage back to the parent
            child_records.extend(child_res["records"])
            child_raw_pages.extend(
                {"_parent_id": pid, "payload": page} for page in child_res["raw_pages"]
            )
            parents_completed += 1

        total_records = len(child_records)
        print(f"Child extraction finished. Parents completed: {parents_completed}/{len(parent_ids)} "
              f"— child records collected: {total_records}")

        # ---------------- Phase 3: bronze write (complete runs only) ----------------
        if total_records > 0:
            print("Flattening child data...")
            pdf = pd.json_normalize(child_records).astype(str)
            pdf.columns = dedupe_column_names(list(pdf.columns))
            pdf["_ingestion_batch_id"] = batch_id
            pdf["_ingestion_ts_utc"] = datetime.now(timezone.utc).isoformat()
            pdf["_source_url"] = api_url
            total_records_written = len(pdf)
            writeFilePandas(
                pdf,
                "bronze",
                destination_raw_file_system,
                destination_raw_file_folder,
                destination_raw_file,
            )
            if write_raw_json:
                destination_raw_json_file_folder = destination_raw_file_folder.replace("raw_bronze", "raw_json_bronze")
                destination_raw_json_file = destination_raw_file.replace("parquet", "json")
                raw_path = (
                    f"{getAbfsPath('bronze')}/{destination_raw_file_system}/"
                    f"{destination_raw_json_file_folder}/{destination_raw_json_file}"
                )
                raw_content = "\n".join(json.dumps(page, default=str) for page in child_raw_pages)
                mssparkutils.fs.put(raw_path, raw_content, True)
                print(f"Raw payload written to: {raw_path}")
        else:
            print("No child records extracted — nothing written for the child stream.")

        run_status = "Success"
        print(f"{run_status}. Child records: {total_records}, parents: {parents_completed}")
        exit_values = {
            "status": run_status,
            "limit_reached": False,
            "batch_id": batch_id,
            "pages_read": parent_res["page_count"] + child_page_total,
            "rows_read": total_records,
            "rows_copied": total_records_written,
            "parents_found": len(parent_ids),
            "parents_completed": parents_completed,
            "new_watermark_date": "1900-01-01",
            "new_watermark_number": 0,
        }
        exit_json = json.dumps(exit_values)
        print(f"Exiting with: {exit_json}")
        mssparkutils.notebook.exit(exit_json)

    elif window_mode:
        # ==================================================================
        # DATE-WINDOW FULL LOAD — e.g. AlayaCare forms submissions (max 14
        # days per call). Page through each window and merge everything;
        # a daily limit mid-sweep → NOTHING is written (all-or-nothing) and
        # the next run starts a fresh full sweep. Silver upserts catch
        # updates to historical submissions (submitted_from/to filter on
        # creation date, so edited records stay in their original window
        # and are re-read by every run).
        # ==================================================================
        window_records = []
        window_raw_pages = []
        windows_completed = 0
        window_page_total = 0
        for idx, (w_from, w_to) in enumerate(windows, 1):
            # upsert REPLACES any from/to values already present in api_url
            window_url = upsert_query_param(api_url, window_param_from, w_from)
            window_url = upsert_query_param(window_url, window_param_to, w_to)
            print(f"--- Window {idx}/{len(windows)}: {window_param_from}={w_from} "
                  f"{window_param_to}={w_to}")
            res = extract_all_pages(window_url, window_url, None, None, None,
                                    label=f"window {idx}")
            window_page_total += res["page_count"]
            if res["max_pages_hit"]:
                raise RuntimeError(
                    f"Window {idx} ({w_from} → {w_to}) reached the max_pages safety stop "
                    f"({max_pages}). Data would be incomplete — nothing was written. "
                    "Increase max_pages, or shrink window_days for this stream.")
            if res["limit_hit"]:
                print(f"API daily limit hit in window {idx}/{len(windows)} "
                      f"({w_from} → {w_to}) — exiting WITHOUT writing anything. "
                      "The next run starts a fresh full sweep.")
                exit_values = {
                    "status": "Partial",
                    "limit_reached": True,
                    "batch_id": batch_id,
                    "pages_read": window_page_total,
                    "rows_read": len(window_records),
                    "rows_copied": 0,
                    "windows_completed": windows_completed,
                    "windows_total": len(windows),
                    "new_watermark_date": "1900-01-01",
                    "new_watermark_number": 0,
                }
                exit_json = json.dumps(exit_values)
                print(f"Exiting with: {exit_json}")
                mssparkutils.notebook.exit(exit_json)
            window_records.extend(res["records"])
            window_raw_pages.extend(
                {"_window_from": w_from, "_window_to": w_to, "payload": page}
                for page in res["raw_pages"]
            )
            windows_completed += 1

        total_records = len(window_records)
        print(f"Window sweep finished. Windows completed: {windows_completed}/{len(windows)} "
              f"— records collected: {total_records}")

        if total_records > 0:
            print("Flattening data...")
            pdf = pd.json_normalize(window_records).astype(str)
            pdf.columns = dedupe_column_names(list(pdf.columns))
            pdf["_ingestion_batch_id"] = batch_id
            pdf["_ingestion_ts_utc"] = datetime.now(timezone.utc).isoformat()
            pdf["_source_url"] = api_url
            total_records_written = len(pdf)
            writeFilePandas(
                pdf,
                "bronze",
                destination_raw_file_system,
                destination_raw_file_folder,
                destination_raw_file,
            )
            if write_raw_json:
                destination_raw_json_file_folder = destination_raw_file_folder.replace("raw_bronze", "raw_json_bronze")
                destination_raw_json_file = destination_raw_file.replace("parquet", "json")
                raw_path = (
                    f"{getAbfsPath('bronze')}/{destination_raw_file_system}/"
                    f"{destination_raw_json_file_folder}/{destination_raw_json_file}"
                )
                raw_content = "\n".join(json.dumps(page, default=str) for page in window_raw_pages)
                mssparkutils.fs.put(raw_path, raw_content, True)
                print(f"Raw payload written to: {raw_path}")
        else:
            print("No records extracted — nothing written for this stream.")

        run_status = "Success"
        print(f"{run_status}. Records: {total_records}, windows: {windows_completed}/{len(windows)}")
        exit_values = {
            "status": run_status,
            "limit_reached": False,
            "batch_id": batch_id,
            "pages_read": window_page_total,
            "rows_read": total_records,
            "rows_copied": total_records_written,
            "windows_completed": windows_completed,
            "windows_total": len(windows),
            "new_watermark_date": "1900-01-01",
            "new_watermark_number": 0,
        }
        exit_json = json.dumps(exit_values)
        print(f"Exiting with: {exit_json}")
        mssparkutils.notebook.exit(exit_json)

    else:
        # ==================================================================
        # SINGLE-LEVEL STREAM (Epicor / AlayaCare flat endpoints)
        # ==================================================================
        result = extract_all_pages(
            current_url, keyset_base_url,
            global_max_watermark_found, global_max_watermark_parsed,
            watermark_column,
        )
        all_records = result["records"]
        raw_pages = result["raw_pages"]
        page_count = result["page_count"]
        limit_hit = result["limit_hit"]
        global_max_watermark_found = result["max_watermark_raw"]
        global_max_watermark_parsed = result["max_watermark_parsed"]
        total_records = len(all_records)
        print(f"Extraction finished. Total records collected: {total_records}")

    # ------------------------------------------------------------------
    # Flatten & write to bronze
    # ------------------------------------------------------------------
    if total_records > 0:
        print("Flattening data...")
        pdf = pd.json_normalize(all_records).astype(str)
        total_records_written = len(pdf)

        # Clean column names
        pdf.columns = dedupe_column_names(list(pdf.columns))

        # Audit columns
        pdf["_ingestion_batch_id"] = batch_id
        pdf["_ingestion_ts_utc"] = datetime.now(timezone.utc).isoformat()
        pdf["_source_url"] = api_url

        # Watermark sanity — the watermark column must exist in the payload
        if incremental_mode and watermark_column:
            flat_watermark_col = watermark_column.replace(".", "_")
            if flat_watermark_col not in pdf.columns:
                print(f"WARNING: watermark column '{watermark_column}' not found in flattened "
                      f"data (looked for '{flat_watermark_col}'). Watermark will not advance.")

        # Flattened parquet
        writeFilePandas(
            pdf,
            "bronze",
            destination_raw_file_system,
            destination_raw_file_folder,
            destination_raw_file,
        )

        # Raw JSON payload (audit / replay) — JSON Lines, one page per line
        if write_raw_json:
            destination_raw_json_file_folder = destination_raw_file_folder.replace("raw_bronze", "raw_json_bronze")
            destination_raw_json_file = destination_raw_file.replace("parquet", "json")
            raw_path = (
                f"{getAbfsPath('bronze')}/{destination_raw_file_system}/"
                f"{destination_raw_json_file_folder}/{destination_raw_json_file}"
            )
            raw_content = "\n".join(json.dumps(page, default=str) for page in raw_pages)
            # Overwrite if the file already exists, to avoid duplicates downstream
            mssparkutils.fs.put(raw_path, raw_content, True)
            print(f"Raw payload written to: {raw_path}")

        # --------------------------------------------------------------
        # Watermark update
        # --------------------------------------------------------------
        if incremental_mode:
            if limit_hit and not ordering_guaranteed and partial_run_unordered_policy == "hold_watermark":
                # Unordered source (e.g. AlayaCare): max-fetched-value is NOT a safe
                # resume point — rows after it may not have been fetched yet. Hold the
                # watermark; the next run re-fetches the full range (dedupe in silver).
                print("Partial run on a source without guaranteed date ordering — watermark "
                      "NOT advanced (policy: hold_watermark). The next run re-fetches the full "
                      "range so no records can be skipped.")
                final_watermark = to_str(last_watermark, "")
            elif global_max_watermark_found and global_max_watermark_found != to_str(last_watermark):
                print(f"Updating watermark to: {global_max_watermark_found}")
                if limit_hit:
                    print("Partial run — watermark reflects the highest value actually fetched "
                          "(data is ordered), so the next run resumes exactly where this one stopped.")
                final_watermark = global_max_watermark_found
            else:
                print("Max watermark in new data is not newer than existing watermark. No update needed.")
                final_watermark = to_str(last_watermark, "")
    else:
        # Includes the case where the daily limit was hit on the very first call:
        # nothing to write, watermark stays put — next run retries the same range.
        print("No records extracted — nothing written to bronze.")
        final_watermark = to_str(last_watermark, "")

    final_watermark_number = final_watermark if delta_format == "Number" else 0
    final_watermark_date = final_watermark if delta_format == "Date" else "1900-01-01"
    if delta_format == "Number" and final_watermark != "":
        try:
            num = float(final_watermark)
            final_watermark_number = int(num) if num.is_integer() else num
        except (ValueError, TypeError):
            pass
    if delta_format == "Date" and final_watermark:
        # Persist a consistent, Epicor-safe UTC value to the config table —
        # payload values carry local offsets (e.g. 2023-04-11T21:23:44.087+10:00)
        # which Epicor rejects when they come back in as $filter literals
        normalised_wm = normalize_date_watermark(final_watermark)
        if normalised_wm != final_watermark:
            print(f"Watermark normalised to UTC for the config table: {final_watermark} → {normalised_wm}")
        final_watermark_date = normalised_wm

    run_status = "Partial" if limit_hit else "Success"
    print(f"{run_status}. Total records: {total_records}")

    exit_values = {
        "status": run_status,
        "limit_reached": limit_hit,
        "batch_id": batch_id,
        "pages_read": page_count,
        "rows_read": total_records,
        "rows_copied": total_records_written,
        "new_watermark_date": final_watermark_date,
        "new_watermark_number": final_watermark_number,
    }
    exit_json = json.dumps(exit_values)
    print(f"Exiting with: {exit_json}")
    mssparkutils.notebook.exit(exit_json)

except Exception as e:
    print(f"Error: {e}")
    raise e

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Configuration cheat-sheet
#
# ## Epicor Kinetic (per stream)
# | Parameter | Value |
# |---|---|
# | `api_url` | `https://<host>/<instance>/api/v2/odata/<company>/Erp.BO.CustomerSvc/Customers` |
# | `username_secret_name` / `password_secret_name` | Epicor user secrets |
# | `api_key_secret_name` | Epicor API key secret (sent as `X-API-Key`) |
# | `json_data_key` | `value` |
# | `pagination_type` / `pagination_next_url_key` | `next_url` / `@odata.nextLink` |
# | `api_filter_property` / `delta_format` | e.g. `PostedDate` / `Date`, or `SysRevID` / `Number` — **required** for pagination beyond 100 records |
# | `incremental_filter_style` | `odata_filter` |
#
# ## AlayaCare (per stream)
# | Parameter | Value |
# |---|---|
# | `api_url` | e.g. `https://<tenant>/ext/api/v2/employees/employees` |
# | `username_secret_name` / `password_secret_name` | AlayaCare **public key** / **private key** secrets |
# | `api_key_secret_name` | `None` — AlayaCare uses Basic Auth only |
# | `json_data_key` | `items` |
# | `pagination_type` | `page_number` (`page_number_param=page`, `page_size_param=count`) |
# | `incremental_filter_style` / `api_filter_property` | `query_param` / endpoint-specific — scheduler `/visits` uses `start_date_from`/`start_date_to`; `/facility_visits` uses `start_at`/`end_at`; employees only `created_at`. Format `YYYY-MM-DD HH:mm` (UTC default) |
#
# ## AlayaCare multi-level: billing periods → invoices (parent-child mode)
# | Parameter | Value |
# |---|---|
# | `parent_api_url` | `https://<tenant>/ext/api/v2/accounting/billing/periods/` (legacy twin: `.../billing/cycles/`) |
# | `api_url` | `https://<tenant>/ext/api/v2/accounting/billing/periods/{billing_period_id}/invoices` — the `{placeholder}` is filled with each parent id |
# | `parent_id_column` | `id` (default) — the DISTINCT id list is collected from the parent payload |
# | `json_data_key` / `pagination_type` | `items` / `page_number` — applied to BOTH levels |
#
# Behaviour (FULL-LOAD pattern):
# * Only the CHILD records land in bronze (with a `_parent_id` lineage column).
#   The parent endpoint is just a driver — nothing parent-side is written.
# * Every run re-reads everything; the silver layer handles upserts.
#   `new_watermark_number` is always `0` for these streams.
# * **API daily limit hit ANYWHERE (parent scan or any child page)** → the
#   notebook exits `status="Partial"` and writes NOTHING to bronze. The next
#   scheduled run simply starts a fresh full load.
# * `max_pages` is a HARD stop in this mode: reaching it raises an error rather
#   than landing an incomplete full load.
#
# ## AlayaCare three-level: periods → invoices → invoice details
# | Parameter | Value |
# |---|---|
# | `parent_api_url` | `https://<tenant>/ext/api/v2/accounting/billing/periods/` (L1) |
# | `middle_api_url` | `https://<tenant>/ext/api/v2/accounting/billing/periods/{billing_period_id}/invoices` (L2) |
# | `api_url` | `https://<tenant>/ext/api/v2/accounting/billing/periods/invoice/{invoice_id}/details` (L3) |
# | `parent_id_column` / `middle_id_column` | `id` / `id` — distinct ids flow L1 → L2 → L3 |
# | `json_data_key` / `pagination_type` | `items` / `page_number` — applied to L1 and L2; L3 is a single-object GET (no envelope) |
#
# Behaviour (same FULL-LOAD, all-or-nothing pattern as two-level):
# * Distinct level-1 ids drive the level-2 calls; the DISTINCT level-2 ids
#   (deduplicated across all parents) drive one level-3 detail GET each.
# * ONLY the level-3 detail records land in bronze, each with `_parent_id`
#   (level-2/invoice id) and `_grandparent_id` (level-1/period id) lineage.
# * A daily limit at ANY level → `status="Partial"`, zero bytes written;
#   the next run starts a fresh full load. `new_watermark_number` is always `0`.
# * A level-3 404 (e.g. an invoice deleted between the L2 list and the detail
#   call) is logged and skipped — it does not fail the run.
# * NOTE: level 3 costs ONE API call per invoice — watch the volume against
#   your daily quota on full historical loads.
#
# ## AlayaCare bounded date-range: forms submissions (date-window mode)
# | Parameter | Value |
# |---|---|
# | `api_url` | `https://<tenant>/ext/api/v2/tasks/forms20/submissions?form_id=49&with_fields=true` (any `submitted_from/to` already present are replaced per window) |
# | `window_param_from` / `window_param_to` | `submitted_from` / `submitted_to` |
# | `window_start_date` | first day of history, e.g. `2023-01-01` |
# | `window_end_date` | optional — default today (UTC). Stage big initial loads with bounded config rows |
# | `window_days` | `14` (the API's max range per call) |
# | `window_date_format` | `iso_z` (default: `2026-08-13T00:00:00Z`) or `space_minutes` (`2026-08-13 00:00`, scheduler style) |
# | `json_data_key` / `pagination_type` | `items` / `page_number` |
#
# Behaviour:
# * The range start → end is split into contiguous windows (`day0 00:00:00` →
#   `day13 23:59:59`, inclusive bounds matching the API's `>=`/`<=` semantics,
#   all times UTC); each window is paged through and merged.
# * Every run re-reads the WHOLE range — `submitted_from/to` filter on the
#   submission's CREATION date, so edited historical submissions stay in their
#   original window and are re-read (and upserted by silver) every run.
# * A daily limit mid-sweep → `status="Partial"`, zero bytes written; the next
#   run starts a fresh full sweep. `new_watermark_*` are always defaults.
#   The exit JSON adds `windows_completed` / `windows_total`.
# * `max_pages` applies PER WINDOW and is a hard stop in this mode.
#
# ### Epicor Kinetic quirks — handled automatically
# * **HTTP 500 on fractional seconds AND timezone offsets**: Epicor's OData
#   parser rejects datetime literals like `2023-04-16T03:53:16.2233333Z` and
#   `2023-04-11T21:23:44+10:00` — the latter being the format Epicor itself
#   returns in payloads (PostedDate in tenant local time). Every Date watermark
#   going into a `$filter` (incoming `last_watermark`, keyset fallback clauses)
#   and out to the config table (`new_watermark_date`) is normalised to UTC
#   `yyyy-MM-ddTHH:mm:ssZ` — the one format Epicor reliably accepts. Naive
#   config-table values (`2023-04-11 11:23:44.0866667`) are assumed UTC.
# * **100-record cap / missing `@odata.nextLink` / broken `$skip`**: Epicor caps
#   pages (default 100), many services never emit `@odata.nextLink`, and `$skip`
#   is silently ignored on some tenants. Full page without a nextLink →
#   **watermark keyset pagination**: re-query with
#   `$filter=<prop> ge <max of batch>` (Date) or `gt <max of batch>` (Number)
#   until an empty/short page. Boundary overlap from `ge` is deduplicated before
#   writing. Requires `api_filter_property` — the run fails fast with a clear
#   error if it is not set.
# * **Tie stall**: if ≥ one full page of records share the exact same watermark
#   value, `ge` cannot advance. The notebook detects the stall, warns, and
#   escalates that step to `gt` (residual ties at that value are skipped). For
#   tie-heavy streams, use a unique Number property such as `SysRevID`.
# * **`$top` + `$count` conflict**: Epicor ignores `$top` (caps at 100) when
#   `$count=true` is present; `$count` is removed from the URL when `$top` is used.
#
# ## API daily limits & initial historical loads
# APIs enforce daily quotas. For the initial historical pull (and any catch-up),
# the notebook is checkpoint-safe:
# * **Detection** — `api_limit_http_codes` (default `429`) and/or
#   `api_limit_error_substring`. Transient throttling is absorbed first by the
#   retry/back-off session; only an exhausted-retry limit response stops the run.
# * **Graceful stop** — on limit, the loop breaks cleanly and all records fetched
#   so far are written to bronze as usual (flattened parquet + raw JSON).
# * **Safe watermark** — OData incremental pulls are ordered
#   (`$orderby=<prop> asc` auto-appended), so the high watermark = max value
#   actually fetched is a valid resume point.
# * **Unordered sources (AlayaCare)** — AlayaCare's Swagger specs document no sort
#   parameter, so max-fetched-value is NOT a safe resume point. On a limit-cut run
#   the default `partial_run_unordered_policy = "hold_watermark"` returns the
#   watermark unchanged: the next run re-fetches the full range (no gaps possible;
#   dedupe in silver). Set it to `"max_fetched"` only after verifying the endpoint
#   returns rows in ascending date order on your tenant.
# * **Resume** — the exit JSON carries `status: "Partial"`, `limit_reached: true`
#   and `new_watermark_date` / `new_watermark_number`. Persist them to your config
#   table; the next run passes them as `last_watermark` and continues where this
#   run stopped. If the limit is hit on the very first call, nothing is written
#   and the watermark is returned unchanged.
# * Boundary overlap: Date resumes re-fetch records stamped exactly on the
#   watermark (`ge`) — bronze keeps raw data, dedupe in silver. Number resumes
#   are exact (`gt`).
#
# ### AlayaCare historical loads — use date windows
# Because AlayaCare is unordered, a quota-limited historical pull can't resume
# mid-range. The recommended pattern is windowing: configure multiple pipeline
# rows for the same endpoint with bounded date ranges in `api_url`
# (e.g. scheduler visits per quarter:
# `.../visits?start_date_from=2023-01-01 00:00&start_date_to=2023-03-31 23:59`),
# each sized to fit within the daily quota. Each window then completes or safely
# retries as a unit.
#
# ### Tips
# * AlayaCare list responses include `count`, `items_per_page`, `page`,
#   `total_pages` — the loop stops at `total_pages`, or earlier on a short page.
# * Extra filters go straight into `api_url`, e.g. `.../employees?filter=john`.
# * Nested JSON is flattened with dots → single underscores. If a nested path
#   collides with a literal payload key (e.g. `program.client.id` vs
#   `program.client_id` in billing invoices), the NESTED column is renamed with
#   double underscores (`program__client__id`) so column names stay unique —
#   all other streams keep the existing single-underscore convention.
# * Not every AlayaCare endpoint supports date filtering — check the Swagger spec
#   for the endpoint before enabling incremental mode for that stream.
# * Full load with overwrite is a safe pattern for endpoints that do not support
#   incremental loading.
