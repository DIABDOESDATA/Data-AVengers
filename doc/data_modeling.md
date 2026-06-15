# Data Modeling — Healthcare Facility Finder

This document defines the logical and physical data models used across the pipeline: Bronze → Silver → Gold, and the Lakebase target. It includes entity definitions, recommended column types, example DDLs, partitioning and indexing guidance, and notes on constraints, keys and schema evolution.

## Goals

- Provide a clear canonical model for engineers and DBAs.
- Make downstream application access predictable and low-latency.
- Ensure lineage and provenance are preserved through layers.

## Key Entities

1. Facility
   - A healthcare facility record describing name, operator, type, services, contact and location data.
2. Pincode / Location
   - Postal-code reference mapping pincodes to lat/lon, city, district, state.
3. HealthIndicator (district-level)
   - NFHS-5 district statistics used for contextual analytics.
4. FacilitySearch (denormalized)
   - Curated search surface combining facility and location data, optimized for full-text and geo queries.

---

## Entity: `facilities` (source)

- Source catalog: `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities`
- Primary fields (examples):
  - `unique_id` (string) — source stable identifier
  - `name` (string)
  - `organization_type` (string)
  - `officialPhone` / `phone_numbers` (string / array)
  - `email` (string)
  - `websites` (string / array)
  - `address_line1`, `address_city`, `address_stateOrRegion`, `address_zipOrPostcode` (string)
  - `latitude`, `longitude` (strings in source; cast to double in Silver)
  - `services`, `specialties` (string / array)

Notes: source types may be loose; Bronze captures raw values unchanged.

---

## Bronze (landing) model — `*_bronze`

Purpose: preserve raw columns and add ingestion metadata.

Common columns to add (Delta table):
- `ingestion_timestamp` TIMESTAMP
- `source_system` STRING
- `source_catalog` STRING
- `source_schema` STRING
- `source_table` STRING
- `ingestion_id` STRING (UUID for the run)

Example Delta CREATE (recommended workflow is to use saveAsTable writes):

```sql
CREATE TABLE IF NOT EXISTS main.healthcare_facility_finder.facilities_bronze (
  unique_id STRING,
  name STRING,
  organization_type STRING,
  phone_numbers STRING,
  email STRING,
  websites STRING,
  address_line1 STRING,
  address_city STRING,
  address_stateOrRegion STRING,
  address_zipOrPostcode STRING,
  latitude STRING,
  longitude STRING,
  services STRING,
  specialties STRING,
  ingestion_timestamp TIMESTAMP,
  source_catalog STRING,
  source_schema STRING,
  source_table STRING,
  ingestion_id STRING
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/main.db/healthcare_facility_finder/facilities_bronze'
```

Partitioning: partition by `date(ingestion_timestamp)` if ingestion volumes warrant.

---

## Silver model — `*_silver`

Purpose: typed, cleaned, deduplicated and enriched records ready for analytics and curation.

Recommended columns and types (Delta):
- `unique_id STRING` (primary logical key)
- `name STRING`
- `organization_type STRING`
- `phone_numbers ARRAY<STRING>` or STRING (normalized)
- `email STRING`
- `websites ARRAY<STRING>`
- `address_line1 STRING`
- `address_city STRING`
- `address_stateOrRegion STRING`
- `address_zipOrPostcode STRING`
- `address_country STRING`
- `latitude DOUBLE`
- `longitude DOUBLE`
- `has_valid_coordinates BOOLEAN`
- `has_contact_info BOOLEAN`
- `services ARRAY<STRING>`
- `specialties ARRAY<STRING>`
- `transformation_timestamp TIMESTAMP`
- `source_*` metadata (copied from Bronze for lineage)

Physical considerations
- Use Delta `overwrite` per partition or `MERGE` for idempotent updates.
- Store arrays as `ARRAY<STRING>` where multi-value semantics are important; otherwise store as normalized comma-delimited strings (preferred: arrays).

DDL example (conceptual):

```sql
CREATE TABLE IF NOT EXISTS main.healthcare_facility_finder.facilities_silver (
  unique_id STRING,
  name STRING,
  organization_type STRING,
  phone_numbers ARRAY<STRING>,
  email STRING,
  websites ARRAY<STRING>,
  address_line1 STRING,
  address_city STRING,
  address_stateOrRegion STRING,
  address_zipOrPostcode STRING,
  address_country STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  has_valid_coordinates BOOLEAN,
  has_contact_info BOOLEAN,
  services ARRAY<STRING>,
  specialties ARRAY<STRING>,
  transformation_timestamp TIMESTAMP,
  source_catalog STRING,
  source_schema STRING,
  source_table STRING
)
USING DELTA
```

Partitioning: prefer `address_stateOrRegion` or `ingestion_date` depending on query patterns. Use low-to-medium cardinality.

Indexing: Delta Lake doesn't support traditional indexes — use Z-ORDER to optimize for predicate columns.

---

## Gold model — `*_gold` / `facilities_search_gold`

Purpose: denormalized, query- and sync-ready surface for application and Lakebase.

Recommended columns (final curated shape):
- `unique_id VARCHAR` (PK)
- `name TEXT`
- `organization_type VARCHAR`
- `phone_numbers TEXT`
- `email VARCHAR`
- `websites TEXT`
- `address_line1 TEXT`
- `address_city VARCHAR`
- `address_stateOrRegion VARCHAR`
- `address_zipOrPostcode VARCHAR`
- `address_country VARCHAR`
- `latitude DOUBLE`
- `longitude DOUBLE`
- `facility_type_id VARCHAR`
- `operator_type_id VARCHAR`
- `specialties TEXT` (comma-separated or JSON)
- `search_tags TEXT` (tokenized or concatenated searchable text)
- `last_updated TIMESTAMP`

Implementation notes
- Create as a `VIEW` over `facilities_silver` during development. Materialize as a table before Lakebase sync if required.
- Ensure `last_updated` reflects the silver `transformation_timestamp` or computed freshness.

Lakebase (Postgres) DDL example (run in Postgres instance):

```sql
CREATE TABLE IF NOT EXISTS facilities_search (
  unique_id VARCHAR(255) PRIMARY KEY,
  name TEXT NOT NULL,
  organization_type VARCHAR(255),
  phone_numbers TEXT,
  email VARCHAR(255),
  websites TEXT,
  address_line1 TEXT,
  address_city VARCHAR(255),
  address_stateOrRegion VARCHAR(255),
  address_zipOrPostcode VARCHAR(20),
  address_country VARCHAR(100),
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  specialties TEXT,
  search_tags TEXT,
  last_updated TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities_search USING GIN (to_tsvector('english', name));
CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities_search (address_stateOrRegion);
CREATE INDEX IF NOT EXISTS idx_facilities_city ON facilities_search (address_city);
CREATE INDEX IF NOT EXISTS idx_facilities_location ON facilities_search (latitude, longitude);
```

---

## Keys, Uniqueness and Surrogates

- Prefer `unique_id` from source as the primary key. Use it for deduplication in Silver.
- If no stable source id exists, create a deterministic surrogate: `sha2(concat_ws('|', stable_fields), 256)`.
- Avoid relying on auto-increment integers — they are not stable across reprocesses.

## Referential Integrity & Constraints

- Delta Lake does not enforce foreign keys; maintain referential integrity through pipeline logic and tests.
- Document expected relationships (e.g., facility.address_zipOrPostcode -> pincodes.pincode).
- In Lakebase/Postgres, add PK and FK constraints when appropriate to enforce integrity on the serving side.

## Partitioning, Compaction & Performance

- Bronze: partition by `ingestion_date` (date(ingestion_timestamp)) to make backfills and incremental loads efficient.
- Silver: partition by `ingestion_month` or `address_stateOrRegion` depending on common query predicates.
- Gold: often small enough to keep unpartitioned; materialized tables can be partitioned by `address_stateOrRegion` if large.
- Compaction: run `OPTIMIZE` and `ZORDER BY` on query-critical columns in silver/gold.

Examples

```sql
OPTIMIZE main.healthcare_facility_finder.facilities_silver ZORDER BY (address_stateOrRegion, unique_id);
```

## Data Types and JSON

- Use arrays and maps in Delta when the data naturally fits (e.g., `services ARRAY<STRING>`).
- For JSON-rich source fields, store raw JSON in Bronze and parse to columns in Silver.
- For Lakebase, serialize arrays/objects to JSON or delimited text depending on query needs.

## Schema Evolution

- Additive changes: safe (add columns). Coordinate breaking changes via release notes and migration jobs.
- Keep a changelog table `pipeline_schema_changes` capturing migration timestamp, author, and rationale.

## Data Dictionary (brief)

- `unique_id`: stable facility identifier
- `name`: facility name
- `organization_type`: public/private/NGO/etc.
- `phone_numbers`: contact numbers
- `email`: contact email
- `websites`: official sites
- `address_*`: address fields
- `latitude`, `longitude`: geographic coordinates (doubles)
- `services`, `specialties`: categorical lists
- `ingestion_timestamp`, `transformation_timestamp`, `last_updated`: lineage timestamps

## Lineage & Provenance

- Keep `source_*` metadata on Silver and Gold to trace back to Bronze and original marketplace table.
- Log run metadata in a `pipeline_runs` table with `run_id`, `start_ts`, `end_ts`, `status`, `notes`.

## Operational Recommendations

- Maintain a small sample dataset for CI tests (10–100 rows) to validate transformations locally.
- Create automated data validations to run after each pipeline stage.
- Backups: rely on Delta time travel for short-term restores; configure long-term backups via snapshot exports if required.

---

## Next steps I can take for you

- Add this `data_modeling.md` to the top-level `README.md` index.
- Generate SQL DDL migration scripts for adding a sample column or converting a text column to `ARRAY<STRING>`.
- Create a `pipeline_runs` table and initial migration notebook.


*Created by the Data-AVengers helper.*
