"""
MMI Kenya Program
KoBoToolbox → DHIS2 Automated Data Pipeline
============================================
Pulls individual form submissions from KoBoToolbox,
aggregates them by facility and period, then pushes
aggregate values to DHIS2 via the dataValueSets API.

Author: Alex Gatongo Arasa
Version: 1.0
"""

import requests
import json
import logging
from datetime import datetime
from collections import defaultdict

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

KOBO_API_TOKEN  = "5d500e2c9d1051d5de2374b8c353398d1d01d4e7"
KOBO_BASE_URL   = "https://kf.kobotoolbox.org"
HTS_FORM_UID    = "aPVpHP9DWfU9sJXLoggVtN"
MM_FORM_UID     = "aHGFTPeX9EjrwDbkuzdKL3"

DHIS2_BASE_URL  = "http://localhost:8080"
DHIS2_USERNAME  = "admin"
DHIS2_PASSWORD  = "district"

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

# ── HELPERS ───────────────────────────────────────────────────────────────────

def date_to_period(date_str):
    if not date_str:
        return None
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return d.strftime("%Y%m")
    except ValueError:
        log.warning(f"Cannot parse date: {date_str}")
        return None

def get_org_unit(sub_location):
    uid = ORG_UNIT_MAP.get(sub_location)
    if not uid:
        log.warning(f"Unknown sub_location '{sub_location}' — record skipped")
    return uid

def safe_get(record, field, default=None):
    return record.get(field, default)

# ── KOBO API ──────────────────────────────────────────────────────────────────

def get_kobo_submissions(form_uid, form_name):
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
            total = data.get("count", 0)
            log.info(f"  {len(all_submissions)} of {total} submissions retrieved")
            page_url = data.get("next")
            params = {}
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            if code == 401:
                log.error("KoBoToolbox auth failed — check API token")
            elif code == 404:
                log.error(f"Form not found — check UID: {form_uid}")
            else:
                log.error(f"KoBoToolbox error {code}: {e}")
            return []
        except requests.exceptions.ConnectionError:
            log.error("Cannot connect to KoBoToolbox — check internet")
            return []

    return all_submissions

# ── HTS AGGREGATION ───────────────────────────────────────────────────────────

def aggregate_hts(submissions):
    """
    KoBoToolbox field       →  DHIS2 data element
    ─────────────────────────────────────────────
    Any non-declined record →  HTS_TST
    hts_result = positive   →  HTS_TST_POS
    referral_outcome = accepted → HTS_REFERRAL
    art_initiated = yes     →  HTS_LINKED_30
    """
    aggregated = defaultdict(lambda: {
        "HTS_TST": 0, "HTS_TST_POS": 0,
        "HTS_REFERRAL": 0, "HTS_LINKED_30": 0,
    })
    processed = skipped = 0

    for record in submissions:
        period = date_to_period(safe_get(record, "session_date"))
        if not period:
            skipped += 1
            continue

        org_unit = get_org_unit(safe_get(record, "sub_location"))
        if not org_unit:
            skipped += 1
            continue

        hts_result = safe_get(record, "hts_result", "")
        if hts_result == "declined":
            skipped += 1
            continue

        key = (org_unit, period)
        aggregated[key]["HTS_TST"] += 1

        if hts_result == "positive":
            aggregated[key]["HTS_TST_POS"] += 1

        if safe_get(record, "referral_outcome") == "accepted":
            aggregated[key]["HTS_REFERRAL"] += 1

        if safe_get(record, "art_initiated") == "yes":
            aggregated[key]["HTS_LINKED_30"] += 1

        processed += 1

    log.info(f"HTS: {processed} processed, {skipped} skipped, "
             f"{len(aggregated)} org-period groups")
    return aggregated

# ── MENTOR MOTHER AGGREGATION ─────────────────────────────────────────────────

def aggregate_mm(submissions):
    """
    KoBoToolbox field               →  DHIS2 data element
    ──────────────────────────────────────────────────────
    Any record                      →  MM_CONTACTS
    duration >= 30 minutes          →  MM_QUALITY
    Unique client_id per period     →  PMTCT_ENROLLED
    cascade_status in delivery set  →  PMTCT_DELIVERY
    + infant_prophylaxis = yes      →  PMTCT_CASCADE
    cascade_status in eid set       →  EID_ELIGIBLE
    + eid_done = yes                →  EID_DONE
    """
    aggregated = defaultdict(lambda: {
        "MM_CONTACTS": 0, "MM_QUALITY": 0,
        "PMTCT_ENROLLED": 0, "PMTCT_CASCADE": 0,
        "PMTCT_DELIVERY": 0, "EID_DONE": 0, "EID_ELIGIBLE": 0,
    })
    enrolled_tracker = defaultdict(set)
    processed = skipped = 0

    DELIVERY_STATUSES = {"delivered", "eid_pending", "eid_done"}
    EID_STATUSES      = {"eid_pending", "eid_done"}

    for record in submissions:
        period = date_to_period(safe_get(record, "contact_date"))
        if not period:
            skipped += 1
            continue

        org_unit = get_org_unit(safe_get(record, "mm_sub_location"))
        if not org_unit:
            skipped += 1
            continue

        key = (org_unit, period)
        client_id = safe_get(record, "client_id", "")

        aggregated[key]["MM_CONTACTS"] += 1

        try:
            if int(safe_get(record, "contact_duration_minutes", 0)) >= 30:
                aggregated[key]["MM_QUALITY"] += 1
        except (ValueError, TypeError):
            pass

        if client_id and client_id not in enrolled_tracker[key]:
            enrolled_tracker[key].add(client_id)
            aggregated[key]["PMTCT_ENROLLED"] += 1

        cascade = safe_get(record, "cascade_status", "")

        if cascade in DELIVERY_STATUSES:
            aggregated[key]["PMTCT_DELIVERY"] += 1
            if safe_get(record, "infant_prophylaxis_given") == "yes":
                aggregated[key]["PMTCT_CASCADE"] += 1

        if cascade in EID_STATUSES:
            aggregated[key]["EID_ELIGIBLE"] += 1
            if safe_get(record, "eid_done") == "yes":
                aggregated[key]["EID_DONE"] += 1

        processed += 1

    log.info(f"MM: {processed} processed, {skipped} skipped, "
             f"{len(aggregated)} org-period groups")
    return aggregated

# ── PAYLOAD BUILDER ───────────────────────────────────────────────────────────

def build_payload(hts_data, mm_data, default_coc):
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
    log.info(f"Payload: {len(data_values)} data values to push")
    return {"dataValues": data_values}

# ── DHIS2 API ─────────────────────────────────────────────────────────────────

def get_default_coc(session):
    url = f"{DHIS2_BASE_URL}/api/categoryOptionCombos.json"
    r = session.get(url, params={
        "fields": "id,name",
        "filter": "name:eq:default",
        "paging": "false"
    })
    r.raise_for_status()
    items = r.json().get("categoryOptionCombos", [])
    if not items:
        raise ValueError("Default COC not found in DHIS2")
    coc = items[0]["id"]
    log.info(f"Default COC: {coc}")
    return coc

def push_to_dhis2(session, payload):
    r = session.post(
        f"{DHIS2_BASE_URL}/api/dataValueSets",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )
    r.raise_for_status()
    return r.json()

def log_summary(response):
    s = response.get("importCount", {})
    c = response.get("conflicts", [])
    status = response.get("status", "UNKNOWN")
    log.info("─" * 48)
    log.info("DHIS2 IMPORT SUMMARY")
    log.info("─" * 48)
    log.info(f"  Status   : {status}")
    log.info(f"  Imported : {s.get('imported', 0)}")
    log.info(f"  Updated  : {s.get('updated', 0)}")
    log.info(f"  Ignored  : {s.get('ignored', 0)}")
    if c:
        log.warning(f"  Conflicts: {len(c)}")
        for x in c[:3]:
            log.warning(f"    {x.get('object','')} — {x.get('value','')}")
    else:
        log.info("  Conflicts: None")
    log.info("─" * 48)
    return status in ("SUCCESS", "WARNING")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def run_pipeline():
    start = datetime.now()
    log.info("═" * 48)
    log.info("  MMI KENYA — KoBo TO DHIS2 PIPELINE")
    log.info(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("═" * 48)

    # 1 — Connect to DHIS2
    log.info("[1/5] Connecting to DHIS2...")
    session = requests.Session()
    session.auth = (DHIS2_USERNAME, DHIS2_PASSWORD)
    try:
        r = session.get(f"{DHIS2_BASE_URL}/api/me.json")
        r.raise_for_status()
        log.info(f"      OK — {r.json().get('displayName', DHIS2_USERNAME)}")
    except requests.exceptions.ConnectionError:
        log.error("Cannot reach DHIS2 at localhost:8080 — is the server running?")
        return False
    except requests.exceptions.HTTPError as e:
        log.error(f"DHIS2 auth failed ({e.response.status_code})")
        return False

    # 2 — Get default COC
    log.info("[2/5] Fetching default category option combo...")
    try:
        default_coc = get_default_coc(session)
    except Exception as e:
        log.error(f"COC fetch failed: {e}")
        return False

    # 3 — Pull HTS submissions
    log.info("[3/5] Pulling HTS submissions from KoBoToolbox...")
    hts_subs = get_kobo_submissions(HTS_FORM_UID, "Community HTS Register")

    # 4 — Pull Mentor Mother submissions
    log.info("[4/5] Pulling Mentor Mother submissions from KoBoToolbox...")
    mm_subs = get_kobo_submissions(MM_FORM_UID, "Mentor Mother Contact Log")

    total = len(hts_subs) + len(mm_subs)
    if total == 0:
        log.warning("No submissions found. Submit test data in KoBoToolbox first.")
        log.info("Pipeline complete — nothing pushed.")
        return True

    log.info(f"Total submissions: {total} "
             f"(HTS: {len(hts_subs)}, MM: {len(mm_subs)})")

    # 5 — Aggregate and push
    log.info("[5/5] Aggregating and pushing to DHIS2...")
    hts_agg = aggregate_hts(hts_subs)
    mm_agg  = aggregate_mm(mm_subs)
    payload = build_payload(hts_agg, mm_agg, default_coc)

    if not payload["dataValues"]:
        log.warning("Zero values after aggregation — check ORG_UNIT_MAP matches")
        log.warning("your KoBoToolbox sub_location values exactly.")
        return True

    try:
        response = push_to_dhis2(session, payload)
        success  = log_summary(response)
    except Exception as e:
        log.error(f"Push failed: {e}")
        return False

    duration = (datetime.now() - start).seconds
    log.info(f"Done in {duration}s — full log: pipeline.log")
    log.info("═" * 48)
    return success

if __name__ == "__main__":
    exit(0 if run_pipeline() else 1)
