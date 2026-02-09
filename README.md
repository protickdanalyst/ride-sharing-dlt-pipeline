# Ride Sharing DLT Pipeline (Databricks) — Production Medallion Architecture

A production-minded Delta Live Tables (DLT) pipeline implementing a Medallion pattern (Bronze → Silver → Gold) for **City** master data and **Trips** fact data. The pipeline emphasizes **streaming correctness**, **strict contracts**, **quarantine-based data quality**, **schema/type drift observability**, **rescued-data recovery**, and **guardrails** that can block unsafe runs.

---

## Short Briefing

This project ingests raw CSV drops using Auto Loader into **Bronze** with strong ingestion metadata, promotes governed datasets in **Silver** using explicit contracts and quality enforcement, and publishes analyst-ready **Gold** facts/dimensions including both **current** and **as-of** (SCD2-aware) truth models. It includes an operational framework for **late-arrival policy**, **replay**, and **bounded backfill**—plus monitoring tables that quantify drift, quarantine rates, join drop, freshness, corrupt/rescued rates, and rescued key distributions.

---

## Key Outcomes

- **Reliable ingestion** with file lineage + deterministic dedupe
- **Governed Silver layer** with strict schema projection and DQ enforcement
- **Quarantine-first error handling** with reasons for triage/recovery
- **SCD2 City dimension** with both **current** and **as-of** usage patterns
- **Gold facts** suitable for BI and analytics with core business measures
- **Observability tables** and **guardrails** to prevent silent data regressions
- **Rescued data recovery path** for evolving schemas without breaking pipelines

---

## Project Structure

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

## Data Flow

### Bronze — Ingestion & Lineage (Streaming)
**Inputs**
- `CITY_PATH`, `TRIPS_PATH` (Auto Loader CSV)
- optional `TRIPS_BACKFILL_PATH` for bounded backfill

**Outputs**
- `bronze_city`
- `bronze_trips`
- `bronze_trips_backfill` (conditional)

**Guarantees**
- Strong ingestion metadata:
  - `_ingest_ts`, `_ingest_date`, `_ingest_yyyymmdd`
  - `_source_file`, `_source_file_mod_ts`, `_source_file_size`
  - `_ingest_run_id`, `_record_hash`
- Resilient ingestion:
  - `_rescued_data` for schema evolution
  - `_corrupt_record` for parse failures
- Deterministic dedupe using `_record_hash` + watermark

---

### Silver — Governed Contracts, DQ Enforcement, Quarantine
**Outputs**
- `silver_city` (SCD2 via `apply_changes`)
- `silver_trips` (SCD1 via `apply_changes`)
- `quarantine_city`, `quarantine_trips`
- drift/quality observability and rescued profiling tables

**Guarantees**
- Mapping-based contract projection into explicit `StructType` contracts
- Strict data quality enforcement via `expect_or_drop` on critical constraints
- Quarantine routing with `_reject_reason`
- Deterministic late-arrival identification
- Replay and backfill sources are streaming-safe (no batch/stream union)

---

### Gold — BI Ready Facts, Dims, Metrics & Guardrails
**Dimensions**
- `dim_city_current` (current snapshot of SCD2 city)
- `dim_date`

**Facts**
- `fact_trips_current` (joins to current city)
- `fact_trips_asof` (SCD2 as-of join for historical truth)
- `fact_trips_daily` (rollups)

**Operational / Monitoring**
- `bronze_ingest_quality` (corrupt/rescued rates + rescued key counts)
- `join_drop_summary` (current + as-of join integrity)
- `data_quality_summary`, `freshness_summary`
- `ops_run_audit`
- `pipeline_guardrails`
- `quarantine_retention_candidates`

---

## Senior Staff Review — Detailed Checklist

### 1) Architecture & Layering
- [x] Medallion layers are clearly separated (Bronze/Silver/Gold)
- [x] Transformations and governance are isolated in Silver
- [x] Consumption and analytics are isolated in Gold
- [x] Backfill is isolated into a separate input stream to avoid contaminating primary ingestion

**Assessment:** Strong. Clear separation of responsibilities and stable layering.

---

### 2) DLT Streaming Semantics & Correctness
- [x] No multipart DLT dataset references inside `dlt.read*` calls
- [x] No union between streaming and batch DataFrames
- [x] All conditional sources return stream-compatible empty datasets
- [x] Watermarks applied for stateful operations (dedupe / streaming stability)
- [x] SCD2 target schema is not pinned (avoids incompatibility with managed columns)

**Assessment:** Correct DLT patterns are applied consistently.

---

### 3) Schema Contracts & Enforcement
- [x] Silver outputs are projected to explicit contracts (`StructType`)
- [x] Mapping-based transformation provides consistent field standardization
- [x] Strict data quality enforcement via `expect_or_drop`
- [x] Schema drift is logged with missing required columns and unexpected columns
- [x] Type drift is measured (cast-failure detection)

**Assessment:** Strong contract discipline. Observability is mature.

---

### 4) Data Quality Routing & Quarantine
- [x] Invalid records are routed to quarantine tables with `_reject_reason`
- [x] Quarantine is partitioned by `_ingest_yyyymmdd` for lifecycle and performance
- [x] Quarantine rate is measured in Gold
- [x] Guardrails include quarantine-rate thresholds

**Assessment:** Good operational hygiene; quarantine is actionable.

---

### 5) Late-Arrival Policy (Business Logic)
- [x] Late arrivals are identified deterministically using ingest-date window
- [x] Late arrivals quarantined with explicit reason (`late_arrival`)
- [x] Replay path is gated and tagged (`_replay_id`)
- [x] Replay is restricted to late arrivals only

**Assessment:** Clear and controllable; late policy is consistent with streaming realities.

---

### 6) Backfill Design (Safety, Determinism)
- [x] Backfill runs from isolated input (`bronze_trips_backfill`)
- [x] Backfill is bounded via date filters
- [x] Backfill is tagged (`_backfill_id`, `_backfill_reason`)
- [x] Backfill remains streaming-correct and does not introduce batch/stream unions

**Assessment:** Safe and auditable backfill approach.

---

### 7) Metadata, Lineage & Idempotency
- [x] `_source_file` and file metadata are captured for lineage
- [x] `_ingest_run_id` supports run-level tracing
- [x] `_record_hash` supports deterministic dedupe/idempotency
- [x] Metadata fields are consistently propagated into Silver contracts

**Assessment:** Solid lineage foundation for triage and audit.

---

### 8) Rescued Data Recovery (Schema Evolution)
- [x] Bronze captures `_rescued_data` for schema evolution
- [x] Rescued profiling tables show rescued keys and frequency
- [x] Promotion mechanism exists via registry + `promote_from_rescued`
- [x] Gold metrics quantify rescued rate and key distribution

**Assessment:** Practical and controlled approach to evolving schemas.

---

### 9) Observability & Guardrails (Operational Control)
- [x] Bronze ingest quality metrics (corrupt/rescued rates)
- [x] Schema drift logging (missing required + unexpected columns)
- [x] Type drift logging and rollup
- [x] Join integrity metrics (current + as-of)
- [x] Freshness tracking for core datasets
- [x] Unified guardrail table that can fail the pipeline when thresholds are breached

**Assessment:** Strong monitoring surface area; guardrails are comprehensive.

---

### 10) Gold Modeling & Analyst Readiness
- [x] `dim_city_current` supports “current-state” reporting
- [x] `fact_trips_asof` supports historically correct reporting
- [x] `fact_trips_daily` provides pre-aggregated consumption datasets
- [x] Fact includes business measures: distance, fare, ratings, trip_count

**Assessment:** Analyst-friendly and supports both operational and historical truth.

---

### 11) Performance & Storage Hygiene
- [x] Bronze/Silver partitioned by `_ingest_yyyymmdd`
- [x] Gold fact partitioned by `year, month`
- [x] Auto optimize enabled on core tables
- [x] ZORDER hints provided for BI-friendly access paths
- [x] Join drop and unmatched dimension records are measurable for integrity analysis

**Assessment:** Good baseline. As-of joins can be the next performance focus at scale.

---

## Summary of Strengths (Staff View)
- Streaming semantics are correct and stable
- Strong contract projection and strict DQ enforcement in Silver
- Actionable quarantine model with reasons, partitioning, and metrics
- Observability is robust: drift, type drift, freshness, ingest quality, join integrity
- Gold model supports both current-state and historically correct reporting
- Rescued-data recovery workflow supports controlled schema evolution

---

## Review Notes — Focus Areas for Ongoing Hardening

### Operational Lifecycle (Run Completeness)
`ops_run_audit` records run context and parameters. A complementary completion audit (status + per-run output counts) can be scheduled via workflow tasks for full lifecycle coverage.

### Retention Enforcement (Quarantine Lifecycle)
Retention candidates are identified in `quarantine_retention_candidates`. Enforcement (DELETE + optional VACUUM) is typically executed via scheduled operational jobs.

### Historical As-Of Performance
`fact_trips_asof` is correctness-first. At larger scale, consider a precomputed as-of bridge to reduce join cost.

---

## Primary Tables

### Bronze
- `bronze_city`
- `bronze_trips`
- `bronze_trips_backfill` (conditional)

### Silver
- `silver_city` (SCD2)
- `silver_trips` (SCD1)
- `quarantine_city`
- `quarantine_trips`
- `schema_drift_log`
- `type_drift_log`
- `rescued_city_profile`
- `rescued_trips_profile`

### Gold
- `dim_city_current`
- `dim_date`
- `fact_trips_current`
- `fact_trips_asof`
- `fact_trips_daily`
- `pipeline_guardrails`
- `bronze_ingest_quality`
- `join_drop_summary`
- `freshness_summary`
- `data_quality_summary`
- `ops_run_audit`
- `quarantine_retention_candidates`

---

## Quick Start

1. Configure storage paths in `_common/paths.py` and ensure the target catalog/schemas exist.
2. Create a DLT pipeline pointing at the repository.
3. Run an initial **Full Refresh** on first deployment.
4. Monitor:
   - `gold.pipeline_guardrails`
   - `gold.bronze_ingest_quality`
   - `gold.data_quality_summary`
   - `gold.join_drop_summary`
   - `silver.schema_drift_log`
   - `silver.type_drift_log`

---
