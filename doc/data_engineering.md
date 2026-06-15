# Data Engineering Guide — Raw → Bronze → Silver → Gold

This guide documents the team's patterns and runnable examples for moving data from upstream marketplace sources into a curated, low-latency search surface. It's intended for engineers operating and extending the pipeline in Databricks (Delta Lake + Unity Catalog) and for those configuring Lakebase Postgres sync.

Table of contents
1. Goals
2. Architecture & Layers
3. Conventions
4. Ingestion (Raw → Bronze)
5. Transformation (Bronze → Silver)
6. Curation & Serving (Silver → Gold → Lakebase)
7. Operational Best Practices
8. Testing, Monitoring & Alerting
9. Security & Governance
10. CI/CD, Backfills & Runbook
11. Troubleshooting

## 1. Goals

- Reproducible, idempotent pipelines that preserve provenance.
- Clear layer separation: raw (unchanged) → bronze (landing) → silver (cleaned) → gold (curated/served).
- Enforce schema and data-quality checks early to reduce downstream surprises.

## 2. Architecture & Layers

- Source: Databricks marketplace catalog `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset` (immutable source).
- Target Unity Catalog: `main.healthcare_facility_finder` (primary schema for all pipeline tables).

- Layer responsibilities (summary):
   - Raw: read-only source.
   - Bronze: landing tables with ingestion metadata and minimal transformation (`*_bronze`).
   - Silver: cleaned, typed, deduplicated tables with quality flags (`*_silver`).
   - Gold: denormalized views/tables optimized for queries and sync (`*_gold` / `*_search_gold`).

## 3. Conventions

- Naming: `catalog.schema.table_name_layer` (suffix `_bronze/_silver/_gold`).
- Columns: `snake_case` for pipeline-added fields. Keep source column names unchanged in Bronze.
- Keys: use `unique_id` from source when available; otherwise generate stable surrogate via SHA256 of stable fields.
- Timestamps: include `ingestion_timestamp` (bronze), `transformation_timestamp` (silver), `last_updated` (gold).

## 4. Ingestion (Raw → Bronze)

Objective: reliably land source rows into Delta tables while preserving raw values and provenance.

Patterns
- Write mode: prefer `MERGE` for incremental upserts; use `overwrite` for idempotent windowed batches.
- Add metadata: `ingestion_timestamp`, `source_catalog`, `source_schema`, `source_table`, `ingestion_id`.
- Partitioning: choose `ingestion_date` or `year/month` for time-based reprocessing; avoid high-cardinality partitioning.
- Schema handling: prefer explicit `ALTER TABLE ADD COLUMN` over `mergeSchema` for controlled evolution.

Example (idempotent write with `MERGE`):

```python
from pyspark.sql.functions import current_timestamp

src = spark.table("databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")
bronze_df = src.withColumn("ingestion_timestamp", current_timestamp())

# Example MERGE (pseudo-code)
spark.sql("""
MERGE INTO main.healthcare_facility_finder.facilities_bronze t
USING updates AS s
ON t.unique_id = s.unique_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

Operational notes
- Persist raw connector metadata (file name / marketplace manifest) where possible to aid debugging/backfills.

## 5. Transformation (Bronze → Silver)

Objective: standardize types, remove duplicates, enrich with reference data and quality flags.

Steps
1. Read bronze table and map types explicitly.
2. Trim and normalize textual fields (lowercase emails/websites).
3. Parse numeric/coordinate fields and flag invalid rows.
4. Deduplicate by `unique_id` and add `transformation_timestamp`.
5. Enrich: join `pincodes_silver` for missing lat/lon, compute `search_tags`.

Example (Spark):

```python
from pyspark.sql.functions import trim, lower, when, current_timestamp

bronze = spark.table("main.healthcare_facility_finder.facilities_bronze")
silver = (
      bronze
      .withColumn("name", trim(bronze.name))
      .withColumn("email", lower(trim(bronze.email)))
      .withColumn("latitude", when(bronze.latitude.cast('double').isNotNull(), bronze.latitude.cast('double')).otherwise(None))
      .withColumn("has_valid_coordinates", (bronze.latitude.isNotNull() & bronze.longitude.isNotNull()).cast('boolean'))
      .withColumn("transformation_timestamp", current_timestamp())
      .dropDuplicates(['unique_id'])
)
silver.write.mode('overwrite').option('overwriteSchema','true').saveAsTable('main.healthcare_facility_finder.facilities_silver')
```

Quality flags
- `has_valid_coordinates` (boolean)
- `has_contact_info` (phone/email/website)
- `quality_score` (optional composite metric)

## 6. Curation & Serving (Silver → Gold → Lakebase)

Objective: produce a stable, query-efficient surface for the app and sync it to Lakebase Postgres for sub-10ms lookups.

Best practices
- Build `facilities_search_gold` as a view initially. If sync or performance requires, materialize as a managed table.
- Include a `last_updated` timestamp used by sync mechanisms.
- Provide a single canonical view for Lakebase sync to reduce drift.

Lakebase SQL (example)

```sql
CREATE TABLE IF NOT EXISTS facilities_search (
   unique_id VARCHAR(255) PRIMARY KEY,
   name TEXT,
   address_city VARCHAR(255),
   address_stateOrRegion VARCHAR(255),
   latitude DOUBLE PRECISION,
   longitude DOUBLE PRECISION,
   specialties TEXT,
   last_updated TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities_search USING GIN (to_tsvector('english', name));
CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities_search (address_stateOrRegion);
CREATE INDEX IF NOT EXISTS idx_facilities_location ON facilities_search (latitude, longitude);
```

## 7. Operational Best Practices

- Idempotency: design for safe re-runs using `MERGE` and stable keys.
- Optimize: run `OPTIMIZE` and apply `ZORDER BY` on common filter columns after large writes.
- Vacuum policy: run `VACUUM` with retention aligned to governance (e.g., 168 hours) after compaction windows.
- Job orchestration: use Databricks Jobs or an orchestrator (Airflow, Prefect) to sequence notebooks and handle retries.

Commands (examples)

```sql
-- Optimize and z-order
OPTIMIZE main.healthcare_facility_finder.facilities_silver ZORDER BY (unique_id, address_stateOrRegion);

-- Vacuum (run on maintenance window)
VACUUM main.healthcare_facility_finder.facilities_bronze RETAIN 168 HOURS;
```

## 8. Testing, Monitoring & Alerting

- Data tests: assert row-counts, uniqueness, null-rate thresholds in CI or scheduled checks.
- Use Great Expectations or dbt tests for assertions; run tests as part of the job pipeline.
- Collect metrics: ingestion success, row counts, percent valid coordinates, freshness (`max(ingestion_timestamp)`).
- Alerts: trigger on pipeline failures, missing runs, or metric regression.

Example metric check (pseudo):

```python
rows = spark.table('main.healthcare_facility_finder.facilities_silver').count()
if rows == 0:
      alert('No rows in facilities_silver')
```

## 9. Security & Governance

- Use Unity Catalog for ACLs; grant least privilege to app service principals (`USE CATALOG`, `USE SCHEMA`, `SELECT` on gold views/tables).
- Store Lakebase credentials in Databricks Secrets and reference them in `app.yaml`.
- Maintain an audit table that records pipeline runs, user actions, and schema change notes.

## 10. CI/CD, Backfills & Runbook

- CI: run unit tests for transformation logic; run a small-sample pipeline on a dev workspace for integration tests.
- Backfill: implement a parameterized notebook or job that accepts `start_date`/`end_date` and writes to bronze partitions, then runs downstream jobs.
- Runbook steps (one-line):
   1. `00_setup_and_config.py`
   2. `01_bronze_ingestion.py`
   3. `02_silver_transformation.py`
   4. `03_gold_curation.py`
   5. `04_deploy_app.py`

## 11. Troubleshooting

- Slow writes: check small files / partitions; run `OPTIMIZE`.
- Duplicate rows in silver: confirm `unique_id` used for dedupe; fall back to composite hash key.
- Lakebase sync: verify secrets, connectivity, and that gold view schema matches target Postgres schema.

---

If you'd like, I can:
- Add this guide to the top-level `README.md` index.
- Create Databricks Jobs JSON to run the pipeline on a schedule.
- Generate a Great Expectations suite for the key tables.

*Updated by Data-AVengers helper.*
