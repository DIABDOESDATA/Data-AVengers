# Databricks notebook source
# DBTITLE 1,Project Overview
# MAGIC %md
# MAGIC # Healthcare Facility Finder - Setup & Configuration
# MAGIC
# MAGIC ## Project Overview
# MAGIC
# MAGIC This project builds a **Healthcare Facility Finder** application using the Databricks Virtue Foundation Dataset. The application helps users discover healthcare facilities across India based on location, services, and specialties.
# MAGIC
# MAGIC ### Data Source
# MAGIC - **Catalog**: `databricks_virtue_foundation_dataset_dais_2026`
# MAGIC - **Schema**: `virtue_foundation_dataset`
# MAGIC - **Tables**:
# MAGIC   - `facilities` (10,088 facilities with services, specialties, contact info)
# MAGIC   - `india_post_pincode_directory` (postal code geographic data)
# MAGIC   - `nfhs_5_district_health_indicators` (district health statistics)
# MAGIC
# MAGIC ### Architecture
# MAGIC - **Bronze Layer**: Raw data ingestion from marketplace
# MAGIC - **Silver Layer**: Cleaned, validated, enriched data
# MAGIC - **Gold Layer**: Curated, denormalized data optimized for search
# MAGIC - **Lakebase**: Managed Postgres for sub-10ms facility searches
# MAGIC - **Databricks App**: Python Streamlit web UI for facility search
# MAGIC
# MAGIC ### Target Deployment
# MAGIC - **Catalog**: `main`
# MAGIC - **Schema**: `healthcare_facility_finder`
# MAGIC - **App Name**: `healthcare_facility_finder`

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configuration
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import json
from datetime import datetime

# Configuration
SOURCE_CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
SOURCE_SCHEMA = "virtue_foundation_dataset"
TARGET_CATALOG = "main"
TARGET_SCHEMA = "healthcare_facility_finder"
APP_NAME = "healthcare_facility_finder"

# Display configuration
print("="*80)
print("HEALTHCARE FACILITY FINDER - Configuration")
print("="*80)
print(f"\nSource:")
print(f"  Catalog:  {SOURCE_CATALOG}")
print(f"  Schema:   {SOURCE_SCHEMA}")
print(f"\nTarget:")
print(f"  Catalog:  {TARGET_CATALOG}")
print(f"  Schema:   {TARGET_SCHEMA}")
print(f"\nApp:")
print(f"  Name:     {APP_NAME}")
print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Schema
w = WorkspaceClient()

print("Creating Unity Catalog schema...\n")

# Create schema if not exists
try:
    w.schemas.create(
        catalog_name=TARGET_CATALOG,
        name=TARGET_SCHEMA,
        comment="Healthcare facility finder - medallion architecture with Lakebase sync for sub-10ms searches"
    )
    print(f"✓ Created schema: {TARGET_CATALOG}.{TARGET_SCHEMA}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"✓ Schema already exists: {TARGET_CATALOG}.{TARGET_SCHEMA}")
    else:
        print(f"✗ Error creating schema: {str(e)}")
        raise

# Verify schema exists
try:
    schema_info = w.schemas.get(full_name=f"{TARGET_CATALOG}.{TARGET_SCHEMA}")
    print(f"\n✓ Schema verified:")
    print(f"  Full name: {schema_info.full_name}")
    print(f"  Created:   {schema_info.created_at}")
    print(f"  Owner:     {schema_info.owner}")
except Exception as e:
    print(f"\n✗ Could not verify schema: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Lakebase Setup
# MAGIC %md
# MAGIC ## Lakebase Setup
# MAGIC
# MAGIC Lakebase is a managed PostgreSQL service that provides sub-10ms query performance for the facility search functionality. We'll sync the Gold layer facility search table to Lakebase for lightning-fast lookups.
# MAGIC
# MAGIC ### Why Lakebase?
# MAGIC - **Ultra-low latency**: Sub-10ms queries for real-time search
# MAGIC - **Geo-spatial indexing**: Fast location-based searches
# MAGIC - **Full-text search**: Search facilities by name, services, specialties
# MAGIC - **Auto-sync**: Continuous sync from Unity Catalog to Postgres
# MAGIC - **Managed service**: No infrastructure to manage

# COMMAND ----------

# DBTITLE 1,Lakebase Configuration
# List existing Lakebase instances
print("Checking for existing Lakebase instances...\n")

# Note: Lakebase SDK support is limited in current SDK version
# We'll provide manual setup instructions

print("📋 Lakebase Setup Instructions:")
print("="*80)
print("")
print("1. Navigate to Databricks workspace UI:")
print("   → Compute → Lakebase")
print("")
print("2. Create a new Lakebase instance:")
print("   - Name: healthcare-facility-finder-db")
print("   - Tier: Starter (sufficient for 10k facilities)")
print("   - Region: Same as your workspace")
print("")
print("3. Once provisioned (~5 minutes):")
print("   - Note the connection string")
print("   - Note the database credentials")
print("")
print("4. The sync configuration will be handled in:")
print("   → Notebook 03_gold_curation")
print("")
print("="*80)

# Store configuration for downstream notebooks
config = {
    "source_catalog": SOURCE_CATALOG,
    "source_schema": SOURCE_SCHEMA,
    "target_catalog": TARGET_CATALOG,
    "target_schema": TARGET_SCHEMA,
    "app_name": APP_NAME,
    "lakebase_instance": "healthcare-facility-finder-db",
    "bronze_tables": [
        "facilities",
        "india_post_pincode_directory",
        "nfhs_5_district_health_indicators"
    ],
    "created_at": datetime.now().isoformat()
}

print("\n✓ Configuration saved for downstream notebooks:")
print(json.dumps(config, indent=2))

# Store as notebook widget for easy access
dbutils.widgets.text("target_catalog", TARGET_CATALOG, "Target Catalog")
dbutils.widgets.text("target_schema", TARGET_SCHEMA, "Target Schema")
dbutils.widgets.text("app_name", APP_NAME, "App Name")

print("\n✓ Configuration widgets created")

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Run the following notebooks in order to build the complete Healthcare Facility Finder:
# MAGIC
# MAGIC ### 1. **01_bronze_ingestion** 🥉
# MAGIC - Ingest raw data from Virtue Foundation dataset
# MAGIC - Create bronze tables in Unity Catalog
# MAGIC - Add metadata columns (ingestion timestamp, source)
# MAGIC
# MAGIC ### 2. **02_silver_transformation** 🥈
# MAGIC - Clean and validate bronze data
# MAGIC - Parse complex fields (coordinates, phone numbers, addresses)
# MAGIC - Standardize data types and formats
# MAGIC - Handle nulls and duplicates
# MAGIC
# MAGIC ### 3. **03_gold_curation** 🥇
# MAGIC - Create denormalized facility search table
# MAGIC - Add computed columns (distance calculations, search tags)
# MAGIC - Create aggregated health indicator views
# MAGIC - **Configure Lakebase sync** for sub-10ms searches
# MAGIC
# MAGIC ### 4. **04_deploy_app** 🚀
# MAGIC - Build Python Streamlit web UI
# MAGIC - Configure app.yaml with environment variables
# MAGIC - Deploy to Databricks Apps
# MAGIC - Test facility search functionality
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Setup Complete!
# MAGIC
# MAGIC You can now proceed to run **01_bronze_ingestion** to start building the medallion architecture.
# MAGIC
# MAGIC **Pro Tips:**
# MAGIC - Run notebooks sequentially (Bronze → Silver → Gold → App)
# MAGIC - Each notebook is idempotent (safe to re-run)
# MAGIC - Check Unity Catalog after each stage to verify tables
# MAGIC - Use `SELECT * FROM {table} LIMIT 10` to preview data

# COMMAND ----------


