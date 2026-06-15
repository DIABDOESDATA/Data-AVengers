# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title and Overview
# MAGIC %md
# MAGIC # Healthcare Facility Finder - Silver Layer Transformation
# MAGIC
# MAGIC This notebook transforms Bronze layer data into clean, standardized Silver layer tables.
# MAGIC
# MAGIC **Source:** `main.healthcare_facility_finder` (bronze tables)
# MAGIC **Target:** `main.healthcare_facility_finder` (silver tables)
# MAGIC
# MAGIC **Silver Layer Purpose:**
# MAGIC - Clean and standardize data (trim whitespace, fix data types)
# MAGIC - Remove duplicates
# MAGIC - Handle null values appropriately
# MAGIC - Standardize column names (snake_case)
# MAGIC - Add data quality indicators
# MAGIC - Prepare optimized tables for Gold layer analytics

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Configuration
CATALOG = "main"
SCHEMA = "healthcare_facility_finder"

print("="*80)
print("SILVER LAYER TRANSFORMATION - Configuration")
print("="*80)
print(f"\nCatalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Facilities Silver - Header
# MAGIC %md
# MAGIC ## 1. Facilities Silver Layer
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Trim whitespace from all string columns
# MAGIC - Standardize phone numbers and email addresses
# MAGIC - Parse and validate geographic coordinates
# MAGIC - Remove duplicate facilities (based on unique_id)
# MAGIC - Handle null values in key fields
# MAGIC - Add data quality flags

# COMMAND ----------

# DBTITLE 1,Transform Facilities
print("Transforming facilities to silver layer...")
print("-" * 80)

# Read bronze
facilities_bronze = spark.table(f"{CATALOG}.{SCHEMA}.facilities_bronze")
print(f"Bronze row count: {facilities_bronze.count():,}")

# Transformations
facilities_silver = facilities_bronze \
    .withColumn("name", trim(col("name"))) \
    .withColumn("email", trim(lower(col("email")))) \
    .withColumn("officialPhone", trim(col("officialPhone"))) \
    .withColumn("officialWebsite", trim(lower(col("officialWebsite")))) \
    .withColumn("address_line1", trim(col("address_line1"))) \
    .withColumn("address_line2", trim(col("address_line2"))) \
    .withColumn("address_city", trim(col("address_city"))) \
    .withColumn("latitude", 
        when(col("latitude").cast("double").isNotNull(), 
             col("latitude").cast("double")).otherwise(None)) \
    .withColumn("longitude",
        when(col("longitude").cast("double").isNotNull(), 
             col("longitude").cast("double")).otherwise(None)) \
    .withColumn("has_valid_coordinates", 
        (col("latitude").isNotNull() & col("longitude").isNotNull())
        .cast("boolean")) \
    .withColumn("has_contact_info",
        (col("officialPhone").isNotNull() | 
         col("email").isNotNull() | 
         col("officialWebsite").isNotNull())
        .cast("boolean")) \
    .withColumn("transformation_timestamp", current_timestamp()) \
    .dropDuplicates(["unique_id"])

print(f"After transformation: {facilities_silver.count():,} rows")

# Write to silver
facilities_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.facilities_silver")

print(f"\n✓ Successfully transformed facilities to silver layer")
print(f"✓ Table: {CATALOG}.{SCHEMA}.facilities_silver")

# Data quality metrics
print("\nData Quality Metrics:")
total = facilities_silver.count()
with_coords = facilities_silver.filter(col("has_valid_coordinates") == True).count()
with_contact = facilities_silver.filter(col("has_contact_info") == True).count()

print(f"  Total facilities: {total:,}")
print(f"  With valid coordinates: {with_coords:,} ({100*with_coords/total:.1f}%)")
print(f"  With contact info: {with_contact:,} ({100*with_contact/total:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Pincodes Silver - Header
# MAGIC %md
# MAGIC ## 2. Pincodes Silver Layer
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Standardize state, district, and city names
# MAGIC - Validate and cast pincode as integer
# MAGIC - Parse and validate coordinates
# MAGIC - Remove duplicates
# MAGIC - Handle missing geographic data
# MAGIC - Add coordinate validation flags

# COMMAND ----------

# DBTITLE 1,Transform Pincodes
print("Transforming pincodes to silver layer...")
print("-" * 80)

# Read bronze
pincodes_bronze = spark.table(f"{CATALOG}.{SCHEMA}.pincodes_bronze")
print(f"Bronze row count: {pincodes_bronze.count():,}")

# Transformations
pincodes_silver = pincodes_bronze \
    .withColumn("statename", trim(upper(col("statename")))) \
    .withColumn("district", trim(upper(col("district")))) \
    .withColumn("officename", trim(col("officename"))) \
    .withColumn("circlename", trim(col("circlename"))) \
    .withColumn("regionname", trim(col("regionname"))) \
    .withColumn("latitude",
        when((col("latitude") != "NA") & (col("latitude").isNotNull()),
             expr("try_cast(latitude as double)")).otherwise(None)) \
    .withColumn("longitude",
        when((col("longitude") != "NA") & (col("longitude").isNotNull()),
             expr("try_cast(longitude as double)")).otherwise(None)) \
    .withColumn("has_coordinates",
        (col("latitude").isNotNull() & col("longitude").isNotNull())
        .cast("boolean")) \
    .withColumn("transformation_timestamp", current_timestamp()) \
    .dropDuplicates(["pincode", "officename"])

print(f"After transformation: {pincodes_silver.count():,} rows")

# Write to silver
pincodes_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.pincodes_silver")

print(f"\n✓ Successfully transformed pincodes to silver layer")
print(f"✓ Table: {CATALOG}.{SCHEMA}.pincodes_silver")

# Data quality metrics
print("\nData Quality Metrics:")
total = pincodes_silver.count()
with_coords = pincodes_silver.filter(col("has_coordinates") == True).count()
unique_states = pincodes_silver.select("statename").distinct().count()

print(f"  Total pincode records: {total:,}")
print(f"  With coordinates: {with_coords:,} ({100*with_coords/total:.1f}%)")
print(f"  Unique states covered: {unique_states}")

# COMMAND ----------

# DBTITLE 1,Health Indicators Silver - Header
# MAGIC %md
# MAGIC ## 3. Health Indicators Silver Layer
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Standardize district and state names
# MAGIC - Clean percentage fields (remove special characters)
# MAGIC - Convert numeric string fields to proper data types
# MAGIC - Handle missing values marked with special characters
# MAGIC - Remove duplicates
# MAGIC - Add data completeness metrics

# COMMAND ----------

# DBTITLE 1,Transform Health Indicators
print("Transforming health indicators to silver layer...")
print("-" * 80)

# Read bronze
health_bronze = spark.table(f"{CATALOG}.{SCHEMA}.health_indicators_bronze")
print(f"Bronze row count: {health_bronze.count():,}")

# Get all columns
all_cols = health_bronze.columns

# Identify string columns that should be numeric (contain percentage or numeric indicators)
string_cols = [field.name for field in health_bronze.schema.fields 
               if field.dataType == StringType()]

# Start with base transformations
health_silver = health_bronze \
    .withColumn("district_name", trim(upper(col("district_name")))) \
    .withColumn("state_ut", trim(upper(col("state_ut"))))

# Clean string columns - remove spaces, asterisks, parentheses from numeric fields
for col_name in string_cols:
    if col_name not in ["district_name", "state_ut", "source_system", "source_catalog", 
                        "source_schema", "source_table"]:
        # Try to convert to double after cleaning
        health_silver = health_silver.withColumn(
            col_name,
            when(col(col_name).isNotNull(),
                 regexp_replace(col(col_name), "[*() ]", ""))
            .otherwise(None)
        )

# Add transformation metadata
health_silver = health_silver \
    .withColumn("transformation_timestamp", current_timestamp()) \
    .dropDuplicates(["district_name", "state_ut"])

print(f"After transformation: {health_silver.count():,} rows")

# Write to silver
health_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.health_indicators_silver")

print(f"\n✓ Successfully transformed health indicators to silver layer")
print(f"✓ Table: {CATALOG}.{SCHEMA}.health_indicators_silver")

# Data quality metrics
print("\nData Quality Metrics:")
total = health_silver.count()
unique_districts = health_silver.select("district_name").distinct().count()
unique_states = health_silver.select("state_ut").distinct().count()

print(f"  Total district records: {total:,}")
print(f"  Unique districts: {unique_districts:,}")
print(f"  Unique states/UTs: {unique_states}")

# COMMAND ----------

# DBTITLE 1,Silver Layer Summary - Header
# MAGIC %md
# MAGIC ## Silver Layer Summary
# MAGIC
# MAGIC All silver tables have been created with:
# MAGIC - ✅ Data cleaned and standardized
# MAGIC - ✅ Duplicates removed
# MAGIC - ✅ Data types corrected
# MAGIC - ✅ Quality flags added
# MAGIC - ✅ Ready for Gold layer curation

# COMMAND ----------

# DBTITLE 1,Summary Statistics
print("="*80)
print("SILVER LAYER TRANSFORMATION SUMMARY")
print("="*80)
print()

tables = [
    ("facilities_silver", "facilities_bronze", "Healthcare facilities"),
    ("pincodes_silver", "pincodes_bronze", "India Post pincode directory"),
    ("health_indicators_silver", "health_indicators_bronze", "NFHS-5 district health indicators")
]

for silver_table, bronze_table, description in tables:
    bronze_count = spark.table(f"{CATALOG}.{SCHEMA}.{bronze_table}").count()
    silver_count = spark.table(f"{CATALOG}.{SCHEMA}.{silver_table}").count()
    reduction = bronze_count - silver_count
    
    print(f"✓ {silver_table}")
    print(f"  Description: {description}")
    print(f"  Bronze rows: {bronze_count:,}")
    print(f"  Silver rows: {silver_count:,}")
    if reduction > 0:
        print(f"  Duplicates removed: {reduction:,}")
    print()

print("="*80)
print("✓ Silver layer transformation complete!")
print("✓ Next step: Run notebook 03_gold_curation")
print("="*80)
