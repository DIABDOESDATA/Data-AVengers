# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title and Overview
# MAGIC %md
# MAGIC # Healthcare Facility Finder - Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook ingests raw data from the Databricks Virtue Foundation Dataset marketplace catalog into Bronze layer tables.
# MAGIC
# MAGIC **Source:** `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset`
# MAGIC **Target:** `main.healthcare_facility_finder` (bronze tables)
# MAGIC
# MAGIC **Bronze Layer Purpose:**
# MAGIC - Ingest raw data from marketplace without transformation
# MAGIC - Add metadata columns (ingestion timestamp, source system)
# MAGIC - Preserve all original columns and data types
# MAGIC - Foundation for Silver layer transformations

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# Configuration
SOURCE_CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
SOURCE_SCHEMA = "virtue_foundation_dataset"
TARGET_CATALOG = "main"
TARGET_SCHEMA = "healthcare_facility_finder"

print("="*80)
print("BRONZE LAYER INGESTION - Configuration")
print("="*80)
print(f"\nSource: {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Create Schema
# Create target schema if it doesn't exist
print(f"Creating schema {TARGET_CATALOG}.{TARGET_SCHEMA} if not exists...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

print(f"✓ Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready")
print()

# COMMAND ----------

# DBTITLE 1,Facilities Table Section
# MAGIC %md
# MAGIC ## 1. Facilities Table
# MAGIC
# MAGIC Ingesting healthcare facilities data with:
# MAGIC - 51 columns including name, location, contact, services, specialties
# MAGIC - Geographic coordinates for location-based search
# MAGIC - Organization metadata and social media presence

# COMMAND ----------

# DBTITLE 1,Ingest Facilities
print("Ingesting facilities table...")
print("-" * 80)

# Read from source
facilities_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.facilities")

print(f"Source row count: {facilities_df.count():,}")
print(f"Source columns: {len(facilities_df.columns)}")

# Add metadata columns
facilities_bronze = facilities_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("marketplace")) \
    .withColumn("source_catalog", lit(SOURCE_CATALOG)) \
    .withColumn("source_schema", lit(SOURCE_SCHEMA)) \
    .withColumn("source_table", lit("facilities"))

# Write to bronze
facilities_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_bronze")

# Verify
count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_bronze").count()
print(f"\n✓ Successfully ingested {count:,} facilities to bronze layer")
print(f"✓ Table: {TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_bronze")

# Show sample
print("\nSample data (first 3 rows):")
spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_bronze").limit(3).display()

# COMMAND ----------

# DBTITLE 1,Pincode Directory Section
# MAGIC %md
# MAGIC ## 2. Pincode Directory Table
# MAGIC
# MAGIC Ingesting India Post pincode directory with:
# MAGIC - Postal codes mapped to geographic locations
# MAGIC - State, district, and city information
# MAGIC - Essential for location-based facility searches

# COMMAND ----------

# DBTITLE 1,Ingest Pincodes
print("Ingesting pincode directory...")
print("-" * 80)

pincodes_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.india_post_pincode_directory")

print(f"Source row count: {pincodes_df.count():,}")
print(f"Source columns: {len(pincodes_df.columns)}")

pincodes_bronze = pincodes_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("marketplace")) \
    .withColumn("source_catalog", lit(SOURCE_CATALOG)) \
    .withColumn("source_schema", lit(SOURCE_SCHEMA)) \
    .withColumn("source_table", lit("india_post_pincode_directory"))

pincodes_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.pincodes_bronze")

count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.pincodes_bronze").count()
print(f"\n✓ Successfully ingested {count:,} pincodes to bronze layer")
print(f"✓ Table: {TARGET_CATALOG}.{TARGET_SCHEMA}.pincodes_bronze")

print("\nSample data (first 3 rows):")
spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.pincodes_bronze").limit(3).display()

# COMMAND ----------

# DBTITLE 1,Health Indicators Section
# MAGIC %md
# MAGIC ## 3. Health Indicators Table
# MAGIC
# MAGIC Ingesting NFHS-5 district health indicators with:
# MAGIC - 109 columns of district-level health statistics
# MAGIC - Demographics, education, healthcare access metrics
# MAGIC - Family planning, maternal health, child health indicators
# MAGIC - Used for contextual health insights in the app

# COMMAND ----------

# DBTITLE 1,Ingest Health Indicators
print("Ingesting health indicators...")
print("-" * 80)

health_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.nfhs_5_district_health_indicators")

print(f"Source row count: {health_df.count():,}")
print(f"Source columns: {len(health_df.columns)}")

health_bronze = health_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("marketplace")) \
    .withColumn("source_catalog", lit(SOURCE_CATALOG)) \
    .withColumn("source_schema", lit(SOURCE_SCHEMA)) \
    .withColumn("source_table", lit("nfhs_5_district_health_indicators"))

health_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.health_indicators_bronze")

count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.health_indicators_bronze").count()
print(f"\n✓ Successfully ingested {count:,} health indicator records to bronze layer")
print(f"✓ Table: {TARGET_CATALOG}.{TARGET_SCHEMA}.health_indicators_bronze")

print("\nSample data (first 3 rows):")
spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.health_indicators_bronze").limit(3).display()

# COMMAND ----------

# DBTITLE 1,Summary Section
# MAGIC %md
# MAGIC ## Bronze Layer Summary
# MAGIC
# MAGIC All bronze tables have been ingested with:
# MAGIC - ✅ Original data preserved
# MAGIC - ✅ Metadata columns added (ingestion_timestamp, source_system, source_catalog, source_schema, source_table)
# MAGIC - ✅ Ready for Silver layer transformations

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("="*80)
print("BRONZE LAYER INGESTION SUMMARY")
print("="*80)
print()

tables = [
    ("facilities_bronze", "Healthcare facilities"),
    ("pincodes_bronze", "India Post pincode directory"),
    ("health_indicators_bronze", "NFHS-5 district health indicators")
]

total_rows = 0

for table_name, description in tables:
    count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}").count()
    total_rows += count
    print(f"✓ {table_name}")
    print(f"  Description: {description}")
    print(f"  Row count: {count:,}")
    print()

print(f"Total rows ingested: {total_rows:,}")
print()
print("="*80)
print("✓ Bronze layer ingestion complete!")
print("✓ Next step: Run notebook 02_silver_transformation")
print("="*80)
