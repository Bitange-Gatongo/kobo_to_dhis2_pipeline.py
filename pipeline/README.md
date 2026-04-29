# KoBoToolbox → DHIS2 Pipeline — Documentation

## Overview

Automated Python pipeline that pulls individual form submissions from
KoBoToolbox, aggregates them into monthly facility-level totals, and
pushes the aggregate values to DHIS2 via the dataValueSets API.

Eliminates manual data entry between the data collection and reporting
systems — reducing transcription errors, compressing the data-to-decision
lag, and creating a documented audit trail for every data transfer.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE FLOW                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  KoBoToolbox API                                            │
│  (individual records)                                       │
│       │                                                     │
│       ▼                                                     │
│  get_kobo_submissions()    ← Authenticated API pull         │
│       │                       with pagination               │
│       ▼                                                     │
│  aggregate_hts() /         ← Count and group by            │
│  aggregate_mm()               org unit + period             │
│       │                                                     │
│       ▼                                                     │
│  build_payload()           ← Format for DHIS2 API          │
│       │                                                     │
│       ▼                                                     │
│  push_to_dhis2()           ← POST to dataValueSets         │
│       │                                                     │
│       ▼                                                     │
│  pipeline.log              ← Full audit trail               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Files

| File | Purpose |
|---|---|
| `kobo_to_dhis2_pipeline.py` | Main pipeline — production use |
| `generate_dhis2_data.py` | Test data generator — development use |
| `pipeline.log` | Auto-generated audit log (created on first run) |

---

## Requirements

```bash
pip install requests python-dateutil
```

Python 3.11 or higher required.

---

## Configuration

Open `kobo_to_dhis2_pipeline.py` and update the configuration block:

```python
# KoBoToolbox credentials
KOBO_API_TOKEN  = "your_token_here"
KOBO_BASE_URL   = "https://kf.kobotoolbox.org"
HTS_FORM_UID    = "your_hts_form_uid"
MM_FORM_UID     = "your_mm_form_uid"

# DHIS2 credentials
DHIS2_BASE_URL  = "http://your-dhis2-server"
DHIS2_USERNAME  = "admin"
DHIS2_PASSWORD  = "your_password"
```

**Finding your KoBoToolbox API token:**
Account Settings → API Key

**Finding your form UIDs:**
```
https://kf.kobotoolbox.org/api/v2/assets/?format=json
```
Look for the `uid` field in each form's JSON object.

---

## Transformation Logic

### HTS Form Aggregation

Individual KoBoToolbox submissions are counted and grouped by
organisation unit and monthly period:

| KoBoToolbox field/condition | DHIS2 data element |
|---|---|
| Any non-declined submission | HTS_TST |
| `hts_result = positive` | HTS_TST_POS |
| `referral_outcome = accepted` | HTS_REFERRAL |
| `art_initiated = yes` | HTS_LINKED_30 |

**Period mapping:**
`session_date` field (YYYY-MM-DD) → DHIS2 period (YYYYMM)

**Org unit mapping:**
`sub_location` field value → DHIS2 org unit UID via `ORG_UNIT_MAP`

**Declined exclusion:**
Records where `hts_result = declined` are excluded from HTS_TST
per PEPFAR MER guidance — declined tests are not counted in
the testing denominator.

### Mentor Mother Form Aggregation

| KoBoToolbox field/condition | DHIS2 data element |
|---|---|
| Any submission | MM_CONTACTS |
| `contact_duration_minutes >= 30` | MM_QUALITY |
| Unique `client_id` per period | PMTCT_ENROLLED |
| `cascade_status` in delivery set | PMTCT_DELIVERY |
| + `infant_prophylaxis_given = yes` | PMTCT_CASCADE |
| `cascade_status` in EID set | EID_ELIGIBLE |
| + `eid_done = yes` | EID_DONE |

**Unique client deduplication:**
PMTCT_ENROLLED counts unique `client_id` values per
org-unit/period combination — preventing double-counting
of clients with multiple contact records in one month.

---

## Running the Pipeline

```bash
# Navigate to pipeline directory
cd mmi-kenya-mel-system/pipeline

# Run the pipeline
python kobo_to_dhis2_pipeline.py
```

**Expected output — successful run with submissions:**
```
2024-03-28 09:15:32 | INFO | ════════════════════════════════
2024-03-28 09:15:32 | INFO | MMI KENYA — KoBo TO DHIS2 PIPELINE
2024-03-28 09:15:33 | INFO | [1/5] Connecting to DHIS2...
2024-03-28 09:15:33 | INFO |       OK — John Traore
2024-03-28 09:15:33 | INFO | [2/5] Fetching category option combo...
2024-03-28 09:15:34 | INFO | [3/5] Pulling HTS submissions...
2024-03-28 09:15:34 | INFO |   47 of 47 submissions retrieved
2024-03-28 09:15:35 | INFO | [4/5] Pulling MM submissions...
2024-03-28 09:15:35 | INFO |   23 of 23 submissions retrieved
2024-03-28 09:15:35 | INFO | [5/5] Aggregating and pushing...
2024-03-28 09:15:35 | INFO | HTS: 44 processed, 3 skipped
2024-03-28 09:15:35 | INFO | MM: 21 processed, 2 skipped
2024-03-28 09:15:35 | INFO | Payload: 32 data values to push
2024-03-28 09:15:36 | INFO | ────────────────────────────────
2024-03-28 09:15:36 | INFO | DHIS2 IMPORT SUMMARY
2024-03-28 09:15:36 | INFO | Status   : SUCCESS
2024-03-28 09:15:36 | INFO | Imported : 28
2024-03-28 09:15:36 | INFO | Updated  : 4
2024-03-28 09:15:36 | INFO | Conflicts: None
2024-03-28 09:15:36 | INFO | Done in 4s — full log: pipeline.log
```

**Expected output — no submissions yet:**
```
No submissions found. Submit test data in KoBoToolbox first.
Pipeline complete — nothing pushed.
```

---

## Error Handling

The pipeline handles all common failure modes gracefully:

| Error | Behavior |
|---|---|
| DHIS2 server not running | Logs error, exits cleanly |
| Wrong DHIS2 credentials | Logs auth error, exits cleanly |
| KoBoToolbox token invalid | Logs auth error, returns empty list |
| Form UID not found | Logs 404 error, returns empty list |
| Unknown sub_location value | Logs warning, skips record, continues |
| Unparseable date field | Logs warning, skips record, continues |
| Zero values after aggregation | Logs warning, exits without pushing |

Records that produce errors are skipped individually — the pipeline
continues processing all remaining records rather than stopping.

---

## Audit Trail

Every pipeline run appends to `pipeline.log`:

```
2024-03-28 09:15:32 | INFO | Pipeline started
2024-03-28 09:15:34 | INFO | 47 submissions retrieved: HTS form
2024-03-28 09:15:35 | WARNING | Unknown sub_location 'kibera' — record skipped
2024-03-28 09:15:35 | INFO | HTS: 44 processed, 3 skipped
2024-03-28 09:15:36 | INFO | Imported: 28, Updated: 4
2024-03-28 09:15:36 | INFO | Done in 4s
```

The log file provides:
- Timestamp of every data transfer
- Exact record counts processed and skipped
- Reasons for skipped records
- DHIS2 import summary for each run
- Evidence for RDQA purposes that data entered DHIS2 at a specific time

---

## Scheduling Automation

To run the pipeline automatically on a schedule:

**Windows — Task Scheduler:**
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger — Daily at 06:00
4. Action: Start a program
5. Program: `py`
6. Arguments: `-3.11 C:\path\to\kobo_to_dhis2_pipeline.py`

**Linux/Mac — Cron:**
```bash
# Run daily at 6am
0 6 * * * cd /path/to/pipeline && python kobo_to_dhis2_pipeline.py
```

---

## Extending the Pipeline

**Adding a new data element:**
1. Add the data element UID to the `DE` dictionary
2. Add aggregation logic to `aggregate_hts()` or `aggregate_mm()`
3. Add the aggregated key to `build_payload()`

**Adding a new sub-location:**
1. Add the KoBoToolbox sub_location value and DHIS2 org unit UID
   to the `ORG_UNIT_MAP` dictionary

**Adding a new form:**
1. Create a new `get_kobo_submissions()` call with the new form UID
2. Create a new aggregation function following the HTS/MM pattern
3. Add the new aggregation to `build_payload()`

---

## Why Python

Python was selected over purpose-built ETL tools for this implementation
because it provides full control over the aggregation transformation logic —
particularly the unique client deduplication for PMTCT_ENROLLED and the
cascade status mapping for PMTCT_CASCADE — which standard ETL tools
handle less flexibly. For larger-scale implementations with higher data
volumes, OpenHIM or the DHIS2 Data Exchange App would reduce maintenance
burden without requiring programming expertise.
