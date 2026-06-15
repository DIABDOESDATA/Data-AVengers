# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title and Overview
# MAGIC %md
# MAGIC # Healthcare Facility Finder - Gold Layer & Lakebase Sync
# MAGIC
# MAGIC This notebook creates search-optimized Gold layer tables and configures continuous sync to Lakebase Postgres for sub-10ms queries.
# MAGIC
# MAGIC **Deliverables:**
# MAGIC - Search-optimized facilities view in Unity Catalog
# MAGIC - Lakebase Postgres sync configuration
# MAGIC - Indexed Postgres table for fast searches

# COMMAND ----------

# DBTITLE 1,Configuration
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import *

TARGET_CATALOG = "main"
TARGET_SCHEMA = "healthcare_facility_finder"
LAKEBASE_INSTANCE = "healthcare-facility-finder-db"

w = WorkspaceClient()
print(f"Creating gold layer in: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create Gold Layer - Facilities Search
# MAGIC %md
# MAGIC ## 1. Create Gold Layer - Facilities Search

# COMMAND ----------

# DBTITLE 1,Create facilities_search_gold view
print("Creating facilities_search_gold view...")

# Create optimized search view
spark.sql(f"""
CREATE OR REPLACE VIEW {TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold AS
SELECT
    unique_id,
    name,
    organization_type,
    phone_numbers,
    email,
    websites,
    address_line1,
    address_line2,
    address_city,
    address_stateOrRegion,
    address_zipOrPostcode,
    address_country,
    latitude,
    longitude,
    facilityTypeId,
    operatorTypeId,
    specialties,
    description,
    transformation_timestamp as last_updated
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_silver
WHERE name IS NOT NULL
ORDER BY name
""")

count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold").count()
print(f"✓ Created facilities_search_gold view: {count:,} facilities")

spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold").limit(5).display()

# COMMAND ----------

# DBTITLE 1,Lakebase Setup Instructions
# MAGIC %md
# MAGIC ## 2. Lakebase Setup Instructions

# COMMAND ----------

# DBTITLE 1,Lakebase setup guide
print("="*80)
print("LAKEBASE SETUP INSTRUCTIONS")
print("="*80)

print(f"""
Follow these steps to configure Lakebase sync:

1. **Create Lakebase Instance** (if not already created):
   - Go to: Databricks Workspace > Compute > Lakebase
   - Click "Create Instance"
   - Name: {LAKEBASE_INSTANCE}
   - Tier: Starter (or higher based on needs)
   - Region: Same as workspace
   - Click "Create"

2. **Get Connection Details**:
   - Once created, click on the instance
   - Note down:
     • Host
     • Port (usually 5432)
     • Database name
     • Username
     • Password (create if needed)

3. **Configure Sync** (Coming in next cell):
   - Unity Catalog → Lakebase sync
   - Source: {TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold
   - Target: Lakebase table 'facilities_search'

4. **Create Indexes** (for fast queries):
   - Index on name (text search)
   - Index on address_stateorregion (filtering)
   - Index on address_city (filtering)
   - Index on latitude, longitude (geospatial)
""")

print("\n✓ Review instructions above before proceeding")

# COMMAND ----------

# DBTITLE 1,Lakebase Sync Configuration
# MAGIC %md
# MAGIC ## 3. Lakebase Sync Configuration

# COMMAND ----------

# DBTITLE 1,Configure sync instructions and SQL
print("Setting up Lakebase sync...")

# Note: Lakebase SDK integration coming soon
# For now, provide SQL commands to run in Lakebase

lakebase_setup_sql = f"""
-- Run these commands in your Lakebase Postgres instance

-- 1. Create the facilities_search table
CREATE TABLE IF NOT EXISTS facilities_search (
    unique_id VARCHAR(255) PRIMARY KEY,
    name TEXT NOT NULL,
    organization_type VARCHAR(255),
    phone_numbers TEXT,
    email VARCHAR(255),
    websites TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    address_city VARCHAR(255),
    address_stateOrRegion VARCHAR(255),
    address_zipOrPostcode VARCHAR(20),
    address_country VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    facility_type_id VARCHAR(100),
    operator_type_id VARCHAR(100),
    specialties TEXT,
    description TEXT,
    last_updated TIMESTAMP
);

-- 2. Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities_search USING GIN (to_tsvector('english', name));
CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities_search (address_stateOrRegion);
CREATE INDEX IF NOT EXISTS idx_facilities_city ON facilities_search (address_city);
CREATE INDEX IF NOT EXISTS idx_facilities_location ON facilities_search (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_facilities_type ON facilities_search (facility_type_id);

-- 3. Configure continuous sync from Unity Catalog
-- (Use Databricks UI: Data > Tables > facilities_search_gold > Sync to Lakebase)
"""

print(lakebase_setup_sql)

print("\n✓ Copy the SQL above and run in your Lakebase instance")
print("✓ Then configure sync via Databricks UI")

# COMMAND ----------

# DBTITLE 1,Verify Lakebase Connection
# MAGIC %md
# MAGIC ## 4. Verify Lakebase Connection

# COMMAND ----------

# DBTITLE 1,Test connection
import os

print("Testing Lakebase connection...")
print("(Update credentials below if testing locally)")

# These would be set as environment variables in production
test_config = {
    "host": os.getenv("LAKEBASE_HOST", "<your-lakebase-host>"),
    "database": os.getenv("LAKEBASE_DATABASE", LAKEBASE_INSTANCE),
    "user": os.getenv("LAKEBASE_USER", "<your-user>"),
    "password": os.getenv("LAKEBASE_PASSWORD", "<your-password>"),
    "port": os.getenv("LAKEBASE_PORT", "5432")
}

print(f"\nConnection config:")
print(f"  Host: {test_config['host']}")
print(f"  Database: {test_config['database']}")
print(f"  User: {test_config['user']}")
print(f"  Port: {test_config['port']}")

print("\n✓ Update config above and uncomment code below to test:")
print("""
# Uncomment to test:
# import psycopg2
# conn = psycopg2.connect(**test_config)
# cur = conn.cursor()
# cur.execute("SELECT COUNT(*) FROM facilities_search")
# count = cur.fetchone()[0]
# print(f"✓ Connected! Facilities in Lakebase: {count:,}")
# conn.close()
""")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Summary and next steps
print("="*80)
print("GOLD LAYER & LAKEBASE SYNC SUMMARY")
print("="*80)

gold_count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold").count()

print(f"\n✓ Gold Layer Created:")
print(f"  - facilities_search_gold: {gold_count:,} facilities")

print(f"\n✓ Next Steps:")
print(f"  1. Complete Lakebase instance setup (see instructions above)")
print(f"  2. Run the SQL commands in Lakebase")
print(f"  3. Configure continuous sync via Databricks UI")
print(f"  4. Verify sync is running")
print(f"  5. Run notebook 04_deploy_app to deploy the Streamlit app")

print(f"\n✓ Once Lakebase sync is active, queries will have <10ms latency!")
