# Transportation DLT Pipeline (Databricks) 🚦 — Production Medallion With Guardrails, Drift Intelligence & Recovery

A **staff-level** Delta Live Tables (DLT) implementation of a Medallion architecture (**Bronze → Silver → Gold**) for **City** master data and **Trips** fact data. Built to be more than “working code”: it’s designed to be **operable**, **auditable**, and **evolution-friendly** under real-world data drift.

---

## Why This Project Has Serious Potential

Most pipelines fail in production for boring reasons: silent schema changes, partial loads, late arrivals, dimension mismatches, and no operational visibility. This project tackles those head-on with:

- **Streaming-correct architecture** (no batch/stream traps, state handled safely)
- **Contract-governed Silver** (explicit schemas, enforced expectations, quarantine routing)
- **Schema + type drift intelligence** (proactive detection, not reactive firefighting)
- **Rescued data recovery workflow** (a controlled path from unknown fields → governed fields)
- **Business-ready Gold** with both:
  - **current-state truth** (fast, BI-friendly)
  - **as-of truth** (historically correct, SCD2-aware)

If you extend this into a platform pattern (new datasets plug in via mappings + contracts), it becomes a reusable “DLT factory”.

---

## Quick Brief

**Inputs**
- City CSV drops
- Trips CSV drops
- Optional Trips backfill stream

**Outputs**
- Governed Silver tables with strict contracts + quarantine
- Gold facts/dimensions for BI + analytics
- Monitoring and guardrail tables that quantify pipeline health and can fail unsafe runs

---

## What You Get (Outcomes)

- **Lineage you can trust:** file path, file timestamps, size, run id, record hash
- **Idempotent ingestion:** deterministic dedupe using `_record_hash`
- **Auditability:** run metadata + quality summaries + drift logs
- **Recoverability:** quarantine + late replay + bounded backfill + rescued profiling
- **Analyst usability:** clean dims/facts with measures (distance, fare, ratings, trip counts)
- **Correctness options:** current model vs as-of model

---

## Architecture At a Glance

```text
                 +-----------------------------+
                 |   Raw Files (CSV drops)     |
                 |   - city                    |
                 |   - trips                   |
                 +--------------+--------------+
                                |
                                v
+--------------------+    +--------------------+
| Bronze (Streaming) |    | Bronze Backfill    |
| Auto Loader +      |    | (Streaming)        |
| rescued/corrupt +  |    | bounded window     |
| metadata + dedupe  |    +--------------------+
+---------+----------+
          |
          v
+-----------------------------+
| Silver (Governed)           |
| - contracts via mapping      |
| - expect_or_drop enforcement |
| - quarantine with reasons    |
| - late policy + replay gate  |
+---------+-------------------+
          |
          v
+----------------------------------------------+
| Gold (Consumption)                            |
| - dim_city_current, dim_date                  |
| - fact_trips_current (fast BI)                |
| - fact_trips_asof (historical truth)          |
| - fact_trips_daily (rollups)                  |
| - guardrails + quality summaries              |
+----------------------------------------------+
```

---

## Repository Structure

```text
transportation_pipeline/
├── _config/
│   ├── settings.py
│   └── rescued_field_registry.py
│
├── _common/
│   ├── contracts.py
│   ├── expectations.py
│   ├── dlt_utils.py
│   ├── rescued_utils.py
│   ├── mappings.py
│   ├── transformers.py
│   └── paths.py
│
├── bronze/
│   ├── bronze_city.py
│   ├── bronze_trips.py
│   └── bronze_trips_backfill.py
│
├── silver/
│   ├── schema_drift_log.py
│   ├── type_drift_log.py
│   ├── type_drift_guardrails.py
│   ├── rescued_city_profile.py
│   ├── rescued_trips_profile.py
│   ├── silver_city.py
│   ├── quarantine_city.py
│   ├── silver_trips.py
│   ├── quarantine_trips.py
│   ├── silver_calendar.py
│   └── silver_trips_unmatched_city.py
│
└── gold/
    ├── gold_dimensions.py
    ├── gold_fact_trips_current.py
    ├── gold_fact_trips_asof.py
    ├── gold_fact_trips_daily.py
    ├── gold_bronze_ingest_quality.py
    ├── gold_join_drop_summary.py
    ├── gold_data_quality_summary.py
    ├── gold_freshness_summary.py
    ├── gold_ops_run_audit.py
    ├── gold_quarantine_retention_candidates.py
    └── gold_pipeline_guardrails.py
```

---

## Bronze Layer — What Makes It “Production”
Bronze doesn’t just ingest. It **stabilizes** ingestion:

**Captured per record**
- `_source_file`, `_source_file_mod_ts`, `_source_file_size`
- `_ingest_ts`, `_ingest_date`, `_ingest_yyyymmdd`
- `_ingest_run_id`
- `_record_hash` (deterministic record identity for dedupe)

**Resiliency**
- `_rescued_data` for unknown columns
- `_corrupt_record` for malformed rows
- Dedupe with watermark + `_record_hash`

---

## Silver Layer — Governance You Can Operate
Silver is where correctness becomes enforceable:

- **Explicit contracts** (`StructType`) for City and Trips
- **Mapping-driven projection** (standardization is systematic, not ad hoc)
- **Strict enforcement** (`expect_or_drop`) for required fields and core constraints
- **Quarantine tables** with `_reject_reason` for triage and replay/backfill workflows
- **Late arrivals** handled deterministically and routed explicitly

### Late Arrivals (Policy)
Records older than the defined window are treated as late and quarantined with `late_arrival`.
A gated replay mechanism allows selective re-introduction with a tracked `_replay_id`.

### Backfill (Bounded + Auditable)
Backfill is handled via a **separate streaming source** and bounded by configured dates.
Rows are tagged with `_backfill_id` and `_backfill_reason`.

---

## Gold Layer — Two Truth Models (Current vs As-Of)

### 1) Current Model (fast BI)
`fact_trips_current` joins trips to **current** city dimension values:
- best for dashboards
- fastest joins
- matches “present-day” reporting needs

### 2) As-Of Model (historically correct)
`fact_trips_asof` joins trips to city attributes **as they were at the trip time**:
- correct under SCD2 changes
- required for historical truth / regulated reporting

### Plus: Daily Rollups
`fact_trips_daily` provides a clean consumption layer for analysts and product metrics.

---

## Drift Intelligence & Observability (Staff-Level Feature Set)

### Schema drift detection
`silver.schema_drift_log`
- missing required columns (breaking drift)
- unexpected columns (growth signals)

### Type drift detection
`silver.type_drift_log`
- detects cast failures field-by-field
- summarized by `silver.type_drift_guardrails`

### Rescued profiling
- `silver.rescued_city_profile`
- `silver.rescued_trips_profile`
Shows which fields are landing in rescued and how frequently—this is the input to your controlled promotion plan.

### Quality summaries + Guardrails
Gold tables compute:
- quarantine rates
- corrupt/rescued rates
- join drop (current and as-of)
- freshness SLA indicators
- drift rates

`gold.pipeline_guardrails` centralizes the checks and can stop unsafe runs.

---

## Senior Staff Review Checklist (What This Demonstrates)

### A) Streaming Correctness & Stability
- [x] No batch/stream unions
- [x] Stream-compatible empty sources for conditional flows
- [x] Watermark usage aligned to stateful ops
- [x] SCD2 targets avoid pinned schema conflicts

### B) Governance & Contracts
- [x] Explicit contracts in Silver
- [x] Mapping-driven projection to contract shape
- [x] Strict expectation enforcement for critical fields

### C) Quarantine & Recovery
- [x] Quarantine with actionable reasons
- [x] Late arrival policy + gated replay
- [x] Bounded backfill stream + tagging for audit

### D) Drift & Evolution Readiness
- [x] Schema drift logs (required + unexpected)
- [x] Type drift logs (cast failure detection)
- [x] Rescued profiling + promotion registry hook

### E) Analyst/Business Readiness
- [x] Current + as-of truth models
- [x] Core business measures present
- [x] Daily aggregates for consumption

### F) Operability
- [x] Run context tracking via `ops_run_audit`
- [x] Health tables support dashboards and alerting
- [x] Guardrails unify operational signals into one decision point

---

## High-Impact Next Enhancements (Roadmap-Level)

These aren’t “missing basics”—they’re how you scale from strong pipeline → strong platform:

1. **As-of join acceleration**
   - build a date_key bridge or precomputed validity map for the city dimension

2. **Closed-loop replay registry**
   - record replay criteria + output counts; optionally mark replayed quarantine rows

3. **Retention enforcement job**
   - `quarantine_retention_candidates` already provides visibility; scheduled deletes make it complete

4. **Operational dashboards**
   - a simple dashboard over guardrails + freshness + drift + quarantine is high ROI

---

## Quick Start

1. Set storage paths in `_common/paths.py` and ensure the target catalog/schemas exist.
2. Create a DLT pipeline pointing at the repository.
3. Run an initial **Full Refresh** on first deployment.
4. Watch:
   - `gold.pipeline_guardrails`
   - `gold.bronze_ingest_quality`
   - `silver.schema_drift_log`
   - `silver.type_drift_log`
   - `gold.join_drop_summary`
   - `gold.freshness_summary`

---

## License
Choose a license appropriate for the repository (MIT/Apache-2.0 are common choices).
