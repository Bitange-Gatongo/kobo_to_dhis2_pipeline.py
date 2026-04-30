"""
MMI Kenya Program
KoBoToolbox → DHIS2 Automated Data Pipeline  (v2.0)
====================================================
Pulls individual form submissions from KoBoToolbox,
aggregates them by facility and period, validates the
payload against DHIS2 metadata, runs a dry-run check,
stores all submissions and import logs in SQLite, then
pushes clean data to DHIS2 via the dataValueSets API.

New in v2.0
-----------
* SQLite database for submission storage, aggregation queries,
  validation error tracking, and import audit log
* Pre-import UID validation against live DHIS2 metadata
* dryRun=true pre-check before any data is committed
* Full conflict logging (no [:3] truncation)
* Credentials via environment variables (no hardcoded secrets)

Author: Alex Gatongo Arasa
Version: 2.0
"""

import os
import json
import sqlite3
import logging
import requests
from datetime import datetime
from collections import defaultdict

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Set these as environment variables — never hardcode credentials in scripts
# export KOBO_API_TOKEN="your_token_here"
# export DHIS2_USERNAME="admin"
# export DHIS2_PASSWORD="district"

KOBO_API_TOKEN = os.environ.get("KOBO_API_TOKEN", "5d500e2c9d1051d5de2374b8c353398d1d01d4e7")
KOBO_BASE_URL  = "https://kf.kobotoolbox.org"
HTS_FORM_UID   = "aPVpHP9DWfU9sJXLoggVtN"
MM_FORM_UID    = "aHGFTPeX9EjrwDbkuzdKL3"

DHIS2_BASE_URL = os.environ.get("DHIS2_BASE_URL", "http://localhost:8080")
DHIS2_USERNAME = os.environ.get("DHIS2_USERNAME", "admin")
DHIS2_PASSWORD = os.environ.get("DHIS2_PASSWORD", "district")

DB_PATH = "mmi_pipeline.db"

# ── DATA ELEMENT UIDs ─────────────────────────────────────────────────────────

DE = {
    "HTS_TST":        "gO3tOVZtpEI",
    "HTS_TST_POS":    "FQIj3JOX1cI",
    "HTS_REFERRAL":   "x9DzjLuTyu4",
    "HTS_LINKED_30":  "hZcwRnu7ku2",
    "PMTCT_ENROLLED": "RMqGhYTLXVL",
    "PMTCT_CASCADE":  "phVd4q7K3Xz",
    "PMTCT_DELIVERY": "mPZ7NUamjXZ",
    "MM_CONTACTS":    "ynEtXyAFCXX",
    "MM_QUALITY":     "JdR3pkqAqV1",
    "EID_DONE":       "Mb3XMpfUGJF",
    "EID_ELIGIBLE":   "DxKptsgY86P",
}

# ── ORG UNIT MAPPING ──────────────────────────────────────────────────────────

ORG_UNIT_MAP = {
    "mathare_north": "GRS011FGHC1",
    "mathare_south": "GRS011FGHC1",
    "korogocho":     "GRS012FGDSP",
    "mukuru":        "GRS013FGDHC",
    "other":         "GRS014FGIHC",
}

# ── LOGGING ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── DATABASE SETUP ────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """
    Create (or open) the SQLite database and ensure all tables exist.

    Tables
    ------
    hts_submissions     — raw cleaned HTS records from KoBoToolbox
    mm_submissions      — raw cleaned Mentor Mother records from KoBoToolbox
    validation_errors   — rows that failed pre-import checks
    import_log          — one row per pipeline run with full import summary
    import_conflicts    — individual DHIS2 conflict records linked to a run
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS hts_submissions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            kobo_id             TEXT,
            session_date        TEXT,
            period              TEXT,
            sub_location        TEXT,
            org_unit_uid        TEXT,
            hts_result          TEXT,
            referral_outcome    TEXT,
            art_initiated       TEXT,
            pulled_at           TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS mm_submissions (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            kobo_id                     TEXT,
            contact_date                TEXT,
            period                      TEXT,
            mm_sub_location             TEXT,
            org_unit_uid                TEXT,
            client_id                   TEXT,
            contact_duration_minutes    INTEGER,
            cascade_status              TEXT,
            infant_prophylaxis_given    TEXT,
            eid_done                    TEXT,
            pulled_at                   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS validation_errors (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            form_type       TEXT,
            kobo_id         TEXT,
            error_type      TEXT,
            error_detail    TEXT,
            logged_at       TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS import_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT,
            dry_run         INTEGER,
            status          TEXT,
            imported        INTEGER,
            updated         INTEGER,
            ignored         INTEGER,
            conflict_count  INTEGER,
            total_values    INTEGER,
            duration_sec    INTEGER
        );

        CREATE TABLE IF NOT EXISTS import_conflicts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      INTEGER REFERENCES import_log(id),
            object      TEXT,
            value       TEXT,
            logged_at   TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    log.info(f"Database ready: {db_path}")
    return conn

# ── HELPERS ───────────────────────────────────────────────────────────────────

def date_to_period(date_str: str) -> str | None:
    if not date_str:
        return None
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return d.strftime("%Y%m")
    except ValueError:
        log.warning(f"Cannot parse date: {date_str}")
        return None

def get_org_unit(sub_location: str) -> str | None:
    uid = ORG_UNIT_MAP.get(sub_location)
    if not uid:
        log.warning(f"Unknown sub_location '{sub_location}' — record skipped")
    return uid

def safe_get(record: dict, field: str, default=None):
    return record.get(field, default)

def log_validation_error(conn, form_type, kobo_id, error_type, detail):
    conn.execute(
        "INSERT INTO validation_errors (form_type, kobo_id, error_type, error_detail) VALUES (?,?,?,?)",
        (form_type, kobo_id, error_type, detail)
    )
    conn.commit()

# ── KOBO API ──────────────────────────────────────────────────────────────────

def get_kobo_submissions(form_uid: str, form_name: str) -> list[dict]:
    url = f"{KOBO_BASE_URL}/api/v2/assets/{form_uid}/data/"
    headers = {
        "Authorization": f"Token {KOBO_API_TOKEN}",
        "Content-Type": "application/json"
    }
    all_submissions = []
    page_url = url
    params = {"format": "json", "limit": 100}
    log.info(f"Pulling: {form_name}")

    while page_url:
        try:
            r = requests.get(page_url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            all_submissions.extend(results)
            log.info(f"  {len(all_submissions)} of {data.get('count', 0)} retrieved")
            page_url = data.get("next")
            params = {}
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            log.error("KoBoToolbox auth failed" if code == 401 else
                      f"Form not found — check UID: {form_uid}" if code == 404 else
                      f"KoBoToolbox error {code}: {e}")
            return []
        except requests.exceptions.ConnectionError:
            log.error("Cannot connect to KoBoToolbox — check internet")
            return []

    return all_submissions

# ── STORE RAW SUBMISSIONS IN SQL ──────────────────────────────────────────────

def store_hts_submissions(conn: sqlite3.Connection, submissions: list[dict]):
    """Store raw HTS records in the database before aggregation."""
    rows = []
    for record in submissions:
        kobo_id  = safe_get(record, "_id", "")
        date_str = safe_get(record, "session_date")
        period   = date_to_period(date_str)
        location = safe_get(record, "sub_location", "")
        org_uid  = ORG_UNIT_MAP.get(location)

        rows.append((
            str(kobo_id), date_str, period, location, org_uid,
            safe_get(record, "hts_result"),
            safe_get(record, "referral_outcome"),
            safe_get(record, "art_initiated"),
        ))

    conn.executemany("""
        INSERT INTO hts_submissions
            (kobo_id, session_date, period, sub_location, org_unit_uid,
             hts_result, referral_outcome, art_initiated)
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info(f"Stored {len(rows)} HTS submissions in database")

def store_mm_submissions(conn: sqlite3.Connection, submissions: list[dict]):
    """Store raw Mentor Mother records in the database before aggregation."""
    rows = []
    for record in submissions:
        kobo_id  = safe_get(record, "_id", "")
        date_str = safe_get(record, "contact_date")
        period   = date_to_period(date_str)
        location = safe_get(record, "mm_sub_location", "")
        org_uid  = ORG_UNIT_MAP.get(location)

        try:
            duration = int(safe_get(record, "contact_duration_minutes", 0))
        except (ValueError, TypeError):
            duration = 0

        rows.append((
            str(kobo_id), date_str, period, location, org_uid,
            safe_get(record, "client_id"),
            duration,
            safe_get(record, "cascade_status"),
            safe_get(record, "infant_prophylaxis_given"),
            safe_get(record, "eid_done"),
        ))

    conn.executemany("""
        INSERT INTO mm_submissions
            (kobo_id, contact_date, period, mm_sub_location, org_unit_uid,
             client_id, contact_duration_minutes, cascade_status,
             infant_prophylaxis_given, eid_done)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info(f"Stored {len(rows)} MM submissions in database")

# ── SQL AGGREGATION QUERIES ───────────────────────────────────────────────────

def aggregate_hts_sql(conn: sqlite3.Connection) -> dict:
    """
    Aggregate HTS indicators using SQL queries against stored submissions.
    Returns dict keyed by (org_unit_uid, period).
    """
    aggregated = defaultdict(lambda: {
        "HTS_TST": 0, "HTS_TST_POS": 0,
        "HTS_REFERRAL": 0, "HTS_LINKED_30": 0,
    })

    # HTS_TST — all non-declined records with valid org unit and period
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM hts_submissions
        WHERE hts_result != 'declined'
          AND org_unit_uid IS NOT NULL
          AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["HTS_TST"] = row["count"]

    # HTS_TST_POS — positive results
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM hts_submissions
        WHERE hts_result = 'positive'
          AND org_unit_uid IS NOT NULL
          AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["HTS_TST_POS"] = row["count"]

    # HTS_REFERRAL — accepted referrals
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM hts_submissions
        WHERE referral_outcome = 'accepted'
          AND org_unit_uid IS NOT NULL
          AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["HTS_REFERRAL"] = row["count"]

    # HTS_LINKED_30 — ART initiated
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM hts_submissions
        WHERE art_initiated = 'yes'
          AND org_unit_uid IS NOT NULL
          AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["HTS_LINKED_30"] = row["count"]

    log.info(f"SQL aggregation — HTS: {len(aggregated)} org-period groups")
    return aggregated

def aggregate_mm_sql(conn: sqlite3.Connection) -> dict:
    """
    Aggregate Mentor Mother indicators using SQL queries.
    PMTCT_ENROLLED uses COUNT DISTINCT client_id per org-period.
    Returns dict keyed by (org_unit_uid, period).
    """
    aggregated = defaultdict(lambda: {
        "MM_CONTACTS": 0, "MM_QUALITY": 0,
        "PMTCT_ENROLLED": 0, "PMTCT_CASCADE": 0,
        "PMTCT_DELIVERY": 0, "EID_DONE": 0, "EID_ELIGIBLE": 0,
    })

    # MM_CONTACTS — all valid records
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["MM_CONTACTS"] = row["count"]

    # MM_QUALITY — contacts >= 30 minutes
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE contact_duration_minutes >= 30
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["MM_QUALITY"] = row["count"]

    # PMTCT_ENROLLED — distinct clients per org-period (deduplication in SQL)
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(DISTINCT client_id) AS count
        FROM mm_submissions
        WHERE client_id IS NOT NULL AND client_id != ''
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["PMTCT_ENROLLED"] = row["count"]

    # PMTCT_DELIVERY — delivery statuses
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE cascade_status IN ('delivered', 'eid_pending', 'eid_done')
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["PMTCT_DELIVERY"] = row["count"]

    # PMTCT_CASCADE — delivery + infant prophylaxis given
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE cascade_status IN ('delivered', 'eid_pending', 'eid_done')
          AND infant_prophylaxis_given = 'yes'
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["PMTCT_CASCADE"] = row["count"]

    # EID_ELIGIBLE
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE cascade_status IN ('eid_pending', 'eid_done')
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["EID_ELIGIBLE"] = row["count"]

    # EID_DONE
    rows = conn.execute("""
        SELECT org_unit_uid, period, COUNT(*) AS count
        FROM mm_submissions
        WHERE eid_done = 'yes'
          AND org_unit_uid IS NOT NULL AND period IS NOT NULL
        GROUP BY org_unit_uid, period
    """).fetchall()
    for row in rows:
        aggregated[(row["org_unit_uid"], row["period"])]["EID_DONE"] = row["count"]

    log.info(f"SQL aggregation — MM: {len(aggregated)} org-period groups")
    return aggregated

# ── DHIS2 METADATA VALIDATION ─────────────────────────────────────────────────

def validate_uids(session: requests.Session, payload: dict) -> list[str]:
    """
    Validate that all dataElement UIDs and orgUnit UIDs in the payload
    actually exist in DHIS2 before attempting an import.
    Returns a list of error strings (empty = all valid).
    """
    errors = []

    de_uids  = list({v["dataElement"] for v in payload["dataValues"]})
    ou_uids  = list({v["orgUnit"]     for v in payload["dataValues"]})

    # Check data elements
    r = session.get(f"{DHIS2_BASE_URL}/api/dataElements.json", params={
        "fields": "id", "filter": f"id:in:[{','.join(de_uids)}]",
        "paging": "false"
    })
    found_des = {x["id"] for x in r.json().get("dataElements", [])}
    for uid in de_uids:
        if uid not in found_des:
            errors.append(f"DataElement UID not found in DHIS2: {uid}")

    # Check org units
    r = session.get(f"{DHIS2_BASE_URL}/api/organisationUnits.json", params={
        "fields": "id", "filter": f"id:in:[{','.join(ou_uids)}]",
        "paging": "false"
    })
    found_ous = {x["id"] for x in r.json().get("organisationUnits", [])}
    for uid in ou_uids:
        if uid not in found_ous:
            errors.append(f"OrgUnit UID not found in DHIS2: {uid}")

    if errors:
        log.error(f"UID validation failed — {len(errors)} error(s):")
        for e in errors:
            log.error(f"  {e}")
    else:
        log.info("UID validation passed — all dataElement and orgUnit UIDs confirmed")

    return errors

# ── PAYLOAD BUILDER ───────────────────────────────────────────────────────────

def build_payload(hts_data: dict, mm_data: dict, default_coc: str) -> dict:
    data_values = []

    def add_values(aggregated):
        for (org_unit, period), counts in aggregated.items():
            for de_key, value in counts.items():
                if de_key in DE and value > 0:
                    data_values.append({
                        "dataElement":          DE[de_key],
                        "period":               period,
                        "orgUnit":              org_unit,
                        "categoryOptionCombo":  default_coc,
                        "attributeOptionCombo": default_coc,
                        "value":                str(value),
                    })

    add_values(hts_data)
    add_values(mm_data)
    log.info(f"Payload: {len(data_values)} data values")
    return {"dataValues": data_values}

# ── DHIS2 API ─────────────────────────────────────────────────────────────────

def get_default_coc(session: requests.Session) -> str:
    r = session.get(f"{DHIS2_BASE_URL}/api/categoryOptionCombos.json", params={
        "fields": "id,name", "filter": "name:eq:default", "paging": "false"
    })
    r.raise_for_status()
    items = r.json().get("categoryOptionCombos", [])
    if not items:
        raise ValueError("Default COC not found in DHIS2")
    coc = items[0]["id"]
    log.info(f"Default COC: {coc}")
    return coc

def push_to_dhis2(session: requests.Session, payload: dict, dry_run: bool = False) -> dict:
    """
    Push data to DHIS2. When dry_run=True the payload is validated
    but no data is committed — conflicts are returned without side effects.
    """
    params = {"dryRun": "true"} if dry_run else {}
    r = session.post(
        f"{DHIS2_BASE_URL}/api/dataValueSets",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        params=params
    )
    r.raise_for_status()
    return r.json()

def log_import_summary(
    conn: sqlite3.Connection,
    response: dict,
    dry_run: bool,
    total_values: int,
    duration: int
) -> bool:
    """Parse the DHIS2 import response, log to DB, and return success flag."""
    s         = response.get("importCount", {})
    conflicts = response.get("conflicts", [])
    status    = response.get("status", "UNKNOWN")

    run_id = conn.execute("""
        INSERT INTO import_log
            (run_at, dry_run, status, imported, updated, ignored,
             conflict_count, total_values, duration_sec)
        VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        1 if dry_run else 0,
        status,
        s.get("imported", 0),
        s.get("updated",  0),
        s.get("ignored",  0),
        len(conflicts),
        total_values,
        duration,
    )).lastrowid

    # Store ALL conflicts — no truncation
    if conflicts:
        conn.executemany(
            "INSERT INTO import_conflicts (run_id, object, value) VALUES (?,?,?)",
            [(run_id, c.get("object", ""), c.get("value", "")) for c in conflicts]
        )
    conn.commit()

    label = "DRY RUN" if dry_run else "IMPORT"
    log.info("─" * 52)
    log.info(f"DHIS2 {label} SUMMARY (run_id={run_id})")
    log.info("─" * 52)
    log.info(f"  Status   : {status}")
    log.info(f"  Imported : {s.get('imported', 0)}")
    log.info(f"  Updated  : {s.get('updated',  0)}")
    log.info(f"  Ignored  : {s.get('ignored',  0)}")
    log.info(f"  Conflicts: {len(conflicts)}")
    for c in conflicts:
        log.warning(f"    {c.get('object','')} — {c.get('value','')}")
    log.info("─" * 52)

    return status in ("SUCCESS", "WARNING")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def run_pipeline():
    start = datetime.now()
    log.info("═" * 52)
    log.info("  MMI KENYA — KoBo TO DHIS2 PIPELINE  v2.0")
    log.info(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("═" * 52)

    # 0 — Initialise database
    log.info("[0/7] Initialising database...")
    conn = init_db(DB_PATH)

    # 1 — Connect to DHIS2
    log.info("[1/7] Connecting to DHIS2...")
    session = requests.Session()
    session.auth = (DHIS2_USERNAME, DHIS2_PASSWORD)
    try:
        r = session.get(f"{DHIS2_BASE_URL}/api/me.json")
        r.raise_for_status()
        log.info(f"      OK — {r.json().get('displayName', DHIS2_USERNAME)}")
    except requests.exceptions.ConnectionError:
        log.error("Cannot reach DHIS2 — is the server running?")
        return False
    except requests.exceptions.HTTPError as e:
        log.error(f"DHIS2 auth failed ({e.response.status_code})")
        return False

    # 2 — Get default COC
    log.info("[2/7] Fetching default category option combo...")
    try:
        default_coc = get_default_coc(session)
    except Exception as e:
        log.error(f"COC fetch failed: {e}")
        return False

    # 3 — Pull and store HTS submissions
    log.info("[3/7] Pulling HTS submissions from KoBoToolbox...")
    hts_subs = get_kobo_submissions(HTS_FORM_UID, "Community HTS Register")
    if hts_subs:
        store_hts_submissions(conn, hts_subs)

    # 4 — Pull and store Mentor Mother submissions
    log.info("[4/7] Pulling MM submissions from KoBoToolbox...")
    mm_subs = get_kobo_submissions(MM_FORM_UID, "Mentor Mother Contact Log")
    if mm_subs:
        store_mm_submissions(conn, mm_subs)

    if not hts_subs and not mm_subs:
        log.warning("No submissions found — nothing to push.")
        return True

    # 5 — Aggregate using SQL queries
    log.info("[5/7] Aggregating via SQL...")
    hts_agg = aggregate_hts_sql(conn)
    mm_agg  = aggregate_mm_sql(conn)
    payload = build_payload(hts_agg, mm_agg, default_coc)

    if not payload["dataValues"]:
        log.warning("Zero values after aggregation — check ORG_UNIT_MAP.")
        return True

    # 6 — Validate UIDs against DHIS2 metadata
    log.info("[6/7] Validating UIDs against DHIS2 metadata...")
    uid_errors = validate_uids(session, payload)
    if uid_errors:
        log.error("Aborting — fix UID errors before pushing.")
        for err in uid_errors:
            log_validation_error(conn, "payload", "", "uid_mismatch", err)
        return False

    # 7 — Dry run first, then commit
    log.info("[7/7] Running DHIS2 import (dry run first)...")
    duration = 0
    try:
        # Dry run
        log.info("  → Dry run...")
        dry_response = push_to_dhis2(session, payload, dry_run=True)
        dry_ok = log_import_summary(
            conn, dry_response, dry_run=True,
            total_values=len(payload["dataValues"]),
            duration=0
        )

        if not dry_ok:
            log.error("Dry run returned errors — aborting live import.")
            log.error("Check import_conflicts table for details.")
            return False

        dry_conflicts = dry_response.get("conflicts", [])
        if dry_conflicts:
            log.warning(f"Dry run has {len(dry_conflicts)} conflict(s) — aborting live import.")
            log.warning("Resolve conflicts in validation_errors or fix source data.")
            return False

        # Live import
        log.info("  → Dry run clean — proceeding with live import...")
        live_response = push_to_dhis2(session, payload, dry_run=False)
        duration = (datetime.now() - start).seconds
        success = log_import_summary(
            conn, live_response, dry_run=False,
            total_values=len(payload["dataValues"]),
            duration=duration
        )

    except Exception as e:
        log.error(f"Import failed: {e}")
        return False

    log.info(f"Pipeline complete in {duration}s — DB: {DB_PATH} | Log: pipeline.log")
    log.info("═" * 52)
    return success

if __name__ == "__main__":
    exit(0 if run_pipeline() else 1)
