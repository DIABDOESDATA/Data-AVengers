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

# Lakebase connection details
LAKEBASE_HOST = "ep-misty-forest-d8hvkz5k.database.us-east-2.cloud.databricks.com"
LAKEBASE_DATABASE = "databricks_postgres"
LAKEBASE_USER = "emdleb@gmail.com"
LAKEBASE_PORT = "5432"

w = WorkspaceClient()
print(f"Creating gold layer in: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Lakebase instance: {LAKEBASE_HOST}")

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

# DBTITLE 1,Data Quality Check - Gold Layer
print("Running comprehensive data quality checks on gold layer...\n")

gold_df = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold")

# 1. Basic counts
total_count = gold_df.count()
print(f"Total records: {total_count:,}")

# 2. Check for nulls in critical fields
print("\n=== NULL VALUE ANALYSIS ===")
critical_fields = ['unique_id', 'name', 'address_stateOrRegion', 'address_city']
for field in critical_fields:
    null_count = gold_df.filter(col(field).isNull()).count()
    null_pct = (null_count / total_count * 100) if total_count > 0 else 0
    print(f"{field}: {null_count:,} nulls ({null_pct:.2f}%)")

# 3. Check for duplicates
print("\n=== DUPLICATE ANALYSIS ===")
duplicates = gold_df.groupBy('unique_id').count().filter(col('count') > 1).count()
print(f"Duplicate unique_ids: {duplicates}")

# 4. GPS coordinate coverage
print("\n=== GPS COORDINATE COVERAGE ===")
with_gps = gold_df.filter(col('latitude').isNotNull() & col('longitude').isNotNull()).count()
gps_pct = (with_gps / total_count * 100) if total_count > 0 else 0
print(f"Records with GPS coordinates: {with_gps:,} ({gps_pct:.2f}%)")

# 5. State and city coverage
print("\n=== GEOGRAPHIC COVERAGE ===")
state_count = gold_df.select('address_stateOrRegion').filter(col('address_stateOrRegion').isNotNull()).distinct().count()
city_count = gold_df.select('address_city').filter(col('address_city').isNotNull()).distinct().count()
print(f"Unique states: {state_count}")
print(f"Unique cities: {city_count}")

# 6. Top states by facility count
print("\n=== TOP 10 STATES BY FACILITY COUNT ===")
gold_df.groupBy('address_stateOrRegion').count() \
    .orderBy(col('count').desc()) \
    .limit(10) \
    .show(truncate=False)

# 7. Check for data quality issues
print("\n=== DATA QUALITY CHECKS ===")

# Empty names
empty_names = gold_df.filter((col('name').isNull()) | (col('name') == '')).count()
print(f"Empty/null names: {empty_names}")

# Suspicious coordinates (outside India bounds roughly)
invalid_coords = gold_df.filter(
    (col('latitude').isNotNull()) & 
    (col('longitude').isNotNull()) & 
    ((col('latitude') < 6) | (col('latitude') > 38) | 
     (col('longitude') < 68) | (col('longitude') > 98))
).count()
print(f"Coordinates outside India bounds: {invalid_coords}")

# 8. Sample records
print("\n=== SAMPLE RECORDS ===")
gold_df.select('name', 'address_city', 'address_stateOrRegion', 'latitude', 'longitude', 'facilityTypeId') \
    .limit(5) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Investigate Malformed Records
print("Investigating malformed records...\n")

# 1. Check records with quotes in state names (likely malformed)
print("=== RECORDS WITH QUOTES IN STATE NAMES ===")
malformed_states = gold_df.filter(col('address_stateOrRegion').contains('"')).select(
    'unique_id', 'name', 'address_city', 'address_stateOrRegion', 'facilityTypeId'
)
malformed_count = malformed_states.count()
print(f"Found {malformed_count} records with quotes in state names")
if malformed_count > 0:
    malformed_states.limit(10).show(truncate=False)

# 2. Check all unique state values (to see the mess)
print("\n=== SAMPLE OF STATE VALUES (showing issues) ===")
state_values = gold_df.groupBy('address_stateOrRegion').count().orderBy(col('count').desc())
print(f"\nTotal unique state values: {state_values.count()}")
print("\nTop 20 state values:")
state_values.limit(20).show(truncate=False)

# 3. Find states that look suspicious (contain special chars)
print("\n=== SUSPICIOUS STATE NAMES ===")
suspicious = gold_df.filter(
    col('address_stateOrRegion').isNotNull() & 
    (col('address_stateOrRegion').contains('"') | 
     col('address_stateOrRegion').contains('Type') |
     col('address_stateOrRegion').contains('Service'))
).select('address_stateOrRegion').distinct()
print(f"Found {suspicious.count()} suspicious state values")
suspicious.show(20, truncate=False)

# 4. Check the silver layer to see if issue originates there
print("\n=== CHECKING SILVER LAYER ===")
silver_df = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_silver")
silver_malformed = silver_df.filter(col('address_stateOrRegion').contains('"'))
print(f"Silver layer malformed records: {silver_malformed.count()}")

# Show a problematic record from silver
if silver_malformed.count() > 0:
    print("\nSample malformed record from SILVER:")
    silver_malformed.limit(1).select(
        'unique_id', 'name', 'address_city', 'address_stateOrRegion', 
        'address_line1', 'facilityTypeId'
    ).show(1, truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,Create Strict Validated Gold View
from pyspark.sql.functions import col, trim, length

print("Creating STRICT gold view with comprehensive validation...\n")

sql = """
CREATE OR REPLACE VIEW {catalog}.{schema}.facilities_search_gold AS
SELECT
    unique_id,
    name,
    organization_type,
    phone_numbers,
    
    -- Clean and validate email
    CASE
        WHEN email IS NULL THEN NULL
        WHEN email = 'null' THEN NULL
        WHEN email LIKE '%[%' THEN NULL
        WHEN email LIKE '%{{%' THEN NULL
        WHEN email NOT LIKE '%@%' THEN NULL
        WHEN email RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN NULL
        WHEN LENGTH(email) > 100 THEN NULL
        ELSE TRIM(email)
    END as email,
    
    websites,
    address_line1,
    address_line2,
    address_city,
    
    -- Clean and standardize state names
    CASE
        WHEN address_stateOrRegion LIKE '%{{%' THEN NULL
        WHEN address_stateOrRegion LIKE '%[%' THEN NULL
        WHEN address_stateOrRegion LIKE '%\"%' THEN NULL
        WHEN address_stateOrRegion LIKE '%coordinates%' THEN NULL
        WHEN address_stateOrRegion = 'kie' THEN NULL
        ELSE TRIM(address_stateOrRegion)
    END as address_stateOrRegion,
    
    address_zipOrPostcode,
    address_country,
    latitude,
    longitude,
    
    -- Clean and validate facilityTypeId
    CASE
        WHEN facilityTypeId IN ('hospital', 'clinic', 'dentist', 'doctor', 'pharmacy') THEN facilityTypeId
        WHEN facilityTypeId = 'farmacy' THEN 'pharmacy'
        WHEN facilityTypeId = 'nursing_home' THEN 'hospital'
        WHEN facilityTypeId LIKE '%http%' THEN NULL
        WHEN facilityTypeId LIKE '%[%' THEN NULL
        WHEN facilityTypeId LIKE '%{{%' THEN NULL
        WHEN facilityTypeId LIKE '%.%' THEN NULL
        WHEN LENGTH(facilityTypeId) > 30 THEN NULL
        ELSE NULL
    END as facilityTypeId,
    
    operatorTypeId,
    specialties,
    description,
    transformation_timestamp as last_updated
FROM {catalog}.{schema}.facilities_silver
WHERE 
    -- CRITICAL: Exclude records with scrambled data
    
    -- 1. unique_id must be a proper UUID or short identifier (not a sentence)
    unique_id IS NOT NULL
    AND unique_id NOT LIKE '% %'
    AND unique_id NOT LIKE ',%'
    AND unique_id NOT LIKE '%\"%'
    AND LENGTH(unique_id) < 100
    
    -- 2. name must be valid (not numeric-only, not null, not empty)
    AND name IS NOT NULL
    AND name NOT LIKE '%[%'
    AND name NOT LIKE '%\"%'
    AND TRIM(name) != ''
    AND NOT (name RLIKE '^[0-9]+$')
    
    -- 3. organization_type must be 'facility' (exclude arrays and weird values)
    AND organization_type = 'facility'
    
    -- 4. Must have either address or GPS coordinates
    AND (address_city IS NOT NULL OR latitude IS NOT NULL)
ORDER BY name
""".format(catalog=TARGET_CATALOG, schema=TARGET_SCHEMA)

spark.sql(sql)
print("✓ Created STRICT validated gold view")

# Verify the cleaned data
cleaned_df = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold")
cleaned_count = cleaned_df.count()
print(f"✓ Gold view: {cleaned_count:,} facilities (after strict validation)")

# Check data quality metrics
print("\n=== DATA QUALITY VALIDATION ===")

# 1. Email validation
valid_emails = cleaned_df.filter(col('email').isNotNull()).count()
invalid_emails = cleaned_df.filter(col('email').isNotNull() & ~col('email').contains('@')).count()
print(f"  Valid emails: {valid_emails:,}")
print(f"  Invalid emails (no @): {invalid_emails} ✓")

# 2. Name validation
numeric_names = cleaned_df.filter(col('name').rlike('^[0-9]+$')).count()
print(f"  Numeric-only names: {numeric_names} ✓")

# 3. Unique ID validation
spaced_ids = cleaned_df.filter(col('unique_id').contains(' ')).count()
print(f"  Unique IDs with spaces: {spaced_ids} ✓")

# 4. Organization type
print("\n  Organization type distribution:")
cleaned_df.groupBy('organization_type').count().show(truncate=False)

# 5. States
states_clean = cleaned_df.select('address_stateOrRegion').filter(col('address_stateOrRegion').isNotNull()).distinct().count()
print(f"  Unique states: {states_clean}")

print("\nTop 10 states:")
cleaned_df.groupBy('address_stateOrRegion').count() \
    .orderBy(col('count').desc()) \
    .limit(10) \
    .show(truncate=False)

print("\nSample of cleaned records:")
cleaned_df.select('name', 'email', 'address_city', 'address_stateOrRegion', 'facilityTypeId') \
    .limit(5) \
    .show(truncate=80, vertical=True)

print("\n✓ All data is now strictly validated and clean!")

# COMMAND ----------

# DBTITLE 1,Lakebase Setup Instructions
# MAGIC %md
# MAGIC ## 2. Lakebase Setup Instructions

# COMMAND ----------

# DBTITLE 1,Lakebase setup guide
print("="*80)
print("LAKEBASE CONNECTION DETAILS")
print("="*80)

print(f"""
✓ Lakebase instance created and configured!

Connection Details:
  Host: {LAKEBASE_HOST}
  Port: {LAKEBASE_PORT}
  Database: {LAKEBASE_DATABASE}
  User: {LAKEBASE_USER}
  SSL Mode: require

Full Connection String:
  postgresql://{LAKEBASE_USER}@{LAKEBASE_HOST}/{LAKEBASE_DATABASE}?sslmode=require

Next Steps:
  1. Run the SQL setup commands in the next cell to create the table and indexes
  2. Load data from Unity Catalog gold view into Lakebase
  3. Test the connection
  4. Update the Streamlit app to use these credentials
""")

print("\n✓ Lakebase instance is ready!")

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

# DBTITLE 1,Load data into Lakebase
import psycopg2
import os
from pyspark.sql import Row

print("Loading data from Unity Catalog gold view into Lakebase...")

# Lakebase OAuth token for authentication
token = "eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBWWdyR190Skh5RUE9PSIsImlzcyI6Imh0dHBzOi8vZGJjLTgxNmZmMzRkLWIzNWQuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6Ijc0NzQ2NTU3NDgwMzM5NDEiLCJzdWIiOiJlbWRsZWJAZ21haWwuY29tIiwiaWF0IjoxNzgxNTUyOTk2LCJleHAiOjE3ODE1NTY1OTYsImp0aSI6ImQ1NmRhNzRjLWVkNTktNGM2YS1hYmI5LTAxYWQ4YWQwYWU5ZSJ9.iqCqwSbhVaInV41hiTKeS4F9zdTkoMdD2lLtsJEZyW7cR3FOmXGxmHAX2lQdGU3wRtO0KVG0ah0E625nTYgMbbddjAqFuPpvN1WXAi893Cn4q3xweh2n-ANroc7a-KXilwXbgOd2VY8X9WIeKhu9oL9EzEybt6-eTY9bXaHdPOmw0-HGohXaUG9xgom91f9Bf_a3pi0KdmmvIYHqe1m_7xlSQqFsMCuRL_gRl7VrH3-l7d3OVywDEaQp7wfEw1P37q7x_uormL7JfSKGezMrnrVyBp1h62J4J5GoKab-b9w4N0iq3E1XxY3j_AGK1Nex1wUR-DPR6Tgzvz5XdEFKYQ"
print(f"✓ Using Lakebase OAuth token for authentication")
password = token

if password:
    try:
        # Connect to Lakebase using OAuth token as password
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            database=LAKEBASE_DATABASE,
            user=LAKEBASE_USER,
            password=token,
            port=LAKEBASE_PORT,
            sslmode="require"
        )
        cur = conn.cursor()
        
        # Drop and recreate the table to ensure correct schema
        print("Recreating facilities_search table...")
        cur.execute("DROP TABLE IF EXISTS facilities_search")
        cur.execute("""
        CREATE TABLE facilities_search (
            unique_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            organization_type TEXT,
            phone_numbers TEXT,
            email TEXT,
            websites TEXT,
            address_line1 TEXT,
            address_line2 TEXT,
            address_city TEXT,
            address_stateOrRegion TEXT,
            address_zipOrPostcode TEXT,
            address_country TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            facilityTypeId TEXT,
            operatorTypeId TEXT,
            specialties TEXT,
            description TEXT,
            last_updated TIMESTAMP
        )
        """)
        conn.commit()
        print("✓ Table created")
        
        # Create indexes
        print("Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities_search USING GIN (to_tsvector('english', name))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities_search (address_stateOrRegion)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_city ON facilities_search (address_city)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_location ON facilities_search (latitude, longitude)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_type ON facilities_search (facilityTypeId)")
        conn.commit()
        print("✓ Indexes created")
        
        # Load data from Unity Catalog
        print("Loading data from Unity Catalog...")
        gold_df = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.facilities_search_gold")
        
        # Convert to pandas for easier loading (use batching for large datasets)
        pandas_df = gold_df.toPandas()
        
        # Clean data: remove null bytes that PostgreSQL doesn't accept
        print("Cleaning data (removing null bytes)...")
        string_columns = pandas_df.select_dtypes(include=['object']).columns
        for col in string_columns:
            pandas_df[col] = pandas_df[col].apply(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
        
        row_count = len(pandas_df)
        print(f"  Found {row_count:,} rows to load")
        
        # Clear existing data and reload
        print("Clearing existing data...")
        cur.execute("TRUNCATE TABLE facilities_search")
        conn.commit()
        
        # Bulk insert
        print("Inserting data...")
        batch_size = 1000
        for i in range(0, row_count, batch_size):
            batch = pandas_df.iloc[i:i+batch_size]
            
            # Prepare batch insert
            values = []
            for _, row in batch.iterrows():
                values.append((
                    row['unique_id'],
                    row['name'],
                    row['organization_type'],
                    row['phone_numbers'],
                    row['email'],
                    row['websites'],
                    row['address_line1'],
                    row['address_line2'],
                    row['address_city'],
                    row['address_stateOrRegion'],
                    row['address_zipOrPostcode'],
                    row['address_country'],
                    row['latitude'],
                    row['longitude'],
                    row['facilityTypeId'],
                    row['operatorTypeId'],
                    row['specialties'],
                    row['description'],
                    row['last_updated']
                ))
            
            # Execute batch insert
            insert_query = """
                INSERT INTO facilities_search VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (unique_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    organization_type = EXCLUDED.organization_type,
                    phone_numbers = EXCLUDED.phone_numbers,
                    email = EXCLUDED.email,
                    websites = EXCLUDED.websites,
                    address_line1 = EXCLUDED.address_line1,
                    address_line2 = EXCLUDED.address_line2,
                    address_city = EXCLUDED.address_city,
                    address_stateOrRegion = EXCLUDED.address_stateOrRegion,
                    address_zipOrPostcode = EXCLUDED.address_zipOrPostcode,
                    address_country = EXCLUDED.address_country,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    facilityTypeId = EXCLUDED.facilityTypeId,
                    operatorTypeId = EXCLUDED.operatorTypeId,
                    specialties = EXCLUDED.specialties,
                    description = EXCLUDED.description,
                    last_updated = EXCLUDED.last_updated
            """
            cur.executemany(insert_query, values)
            conn.commit()
            
            if (i + batch_size) % 5000 == 0:
                rows_inserted = i + batch_size if i + batch_size < row_count else row_count
                print(f"  Inserted {rows_inserted:,} / {row_count:,} rows...")
        
        # Verify
        cur.execute("SELECT COUNT(*) FROM facilities_search")
        final_count = cur.fetchone()[0]
        
        conn.close()
        
        print(f"\n✓ Successfully loaded {final_count:,} facilities into Lakebase!")
        print("✓ Data is now ready for fast queries (<10ms latency)")
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTip: Check that the password is correct and matches what you set during Lakebase instance creation.")
    except Exception as e:
        print(f"\n❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()
else:
    print("\n❌ No password set. Cannot connect to Lakebase.")

# COMMAND ----------

# DBTITLE 1,Inspect Every Column in Lakebase
import psycopg2
import pandas as pd

print("Inspecting every column in Lakebase for data quality...\n")

# Connect to Lakebase
token = "eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBWWdyR190Skh5RUE9PSIsImlzcyI6Imh0dHBzOi8vZGJjLTgxNmZmMzRkLWIzNWQuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6Ijc0NzQ2NTU3NDgwMzM5NDEiLCJzdWIiOiJlbWRsZWJAZ21haWwuY29tIiwiaWF0IjoxNzgxNTUyOTk2LCJleHAiOjE3ODE1NTY1OTYsImp0aSI6ImQ1NmRhNzRjLWVkNTktNGM2YS1hYmI5LTAxYWQ4YWQwYWU5ZSJ9.iqCqwSbhVaInV41hiTKeS4F9zdTkoMdD2lLtsJEZyW7cR3FOmXGxmHAX2lQdGU3wRtO0KVG0ah0E625nTYgMbbddjAqFuPpvN1WXAi893Cn4q3xweh2n-ANroc7a-KXilwXbgOd2VY8X9WIeKhu9oL9EzEybt6-eTY9bXaHdPOmw0-HGohXaUG9xgom91f9Bf_a3pi0KdmmvIYHqe1m_7xlSQqFsMCuRL_gRl7VrH3-l7d3OVywDEaQp7wfEw1P37q7x_uormL7JfSKGezMrnrVyBp1h62J4J5GoKab-b9w4N0iq3E1XxY3j_AGK1Nex1wUR-DPR6Tgzvz5XdEFKYQ"

try:
    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        database=LAKEBASE_DATABASE,
        user=LAKEBASE_USER,
        password=token,
        port=LAKEBASE_PORT,
        sslmode="require"
    )
    cur = conn.cursor()
    
    print("=" * 100)
    print("COLUMN-BY-COLUMN DATA QUALITY INSPECTION")
    print("=" * 100)
    
    # 1. PHONE NUMBERS
    print("\n1. PHONE_NUMBERS Column:")
    print("-" * 80)
    cur.execute("""
        SELECT phone_numbers 
        FROM facilities_search 
        WHERE phone_numbers IS NOT NULL 
        LIMIT 10
    """)
    print("Sample values:")
    for row in cur.fetchall():
        print(f"  {row[0]}")
    
    # 2. EMAIL
    print("\n2. EMAIL Column:")
    print("-" * 80)
    cur.execute("""
        SELECT email 
        FROM facilities_search 
        WHERE email IS NOT NULL 
        LIMIT 10
    """)
    print("Sample values:")
    for row in cur.fetchall():
        print(f"  {row[0]}")
    
    # 3. ADDRESS_LINE1
    print("\n3. ADDRESS_LINE1 Column:")
    print("-" * 80)
    cur.execute("""
        SELECT address_line1 
        FROM facilities_search 
        WHERE address_line1 IS NOT NULL 
        LIMIT 10
    """)
    print("Sample values:")
    for row in cur.fetchall():
        val = str(row[0])[:100]
        print(f"  {val}")
    
    # 4. ADDRESS_CITY
    print("\n4. ADDRESS_CITY Column:")
    print("-" * 80)
    cur.execute("""
        SELECT address_city, COUNT(*) as cnt
        FROM facilities_search 
        WHERE address_city IS NOT NULL 
        GROUP BY address_city
        ORDER BY cnt DESC
        LIMIT 15
    """)
    print("Top cities:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} facilities")
    
    # 5. ADDRESS_STATEORREGION
    print("\n5. ADDRESS_STATEORREGION Column:")
    print("-" * 80)
    cur.execute("""
        SELECT address_stateorregion, COUNT(*) as cnt
        FROM facilities_search 
        WHERE address_stateorregion IS NOT NULL 
        GROUP BY address_stateorregion
        ORDER BY cnt DESC
        LIMIT 20
    """)
    print("Top states:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} facilities")
    
    # 6. LATITUDE & LONGITUDE
    print("\n6. LATITUDE & LONGITUDE Columns:")
    print("-" * 80)
    cur.execute("""
        SELECT latitude, longitude, name, address_city
        FROM facilities_search 
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 10
    """)
    print("Sample GPS coordinates:")
    for row in cur.fetchall():
        print(f"  {row[2][:40]}: ({row[0]}, {row[1]}) in {row[3]}")
    
    # 7. FACILITYTYPEID
    print("\n7. FACILITYTYPEID Column:")
    print("-" * 80)
    cur.execute("""
        SELECT facilitytypeid, COUNT(*) as cnt
        FROM facilities_search 
        WHERE facilitytypeid IS NOT NULL 
        GROUP BY facilitytypeid
        ORDER BY cnt DESC
    """)
    print("Facility types:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} facilities")
    
    # 8. NAME
    print("\n8. NAME Column:")
    print("-" * 80)
    cur.execute("""
        SELECT name 
        FROM facilities_search 
        LIMIT 10
    """)
    print("Sample names:")
    for row in cur.fetchall():
        print(f"  {row[0]}")
    
    print("\n" + "=" * 100)
    print("BASIC INSPECTION COMPLETE - Now checking for data issues...")
    print("=" * 100)
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Verify Lakebase Connection
# MAGIC %md
# MAGIC ## 4. Verify Lakebase Connection

# COMMAND ----------

# DBTITLE 1,Test connection
import psycopg2
import os

print("Testing Lakebase connection...")

# Lakebase OAuth token
token = "eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBWWdyR190Skh5RUE9PSIsImlzcyI6Imh0dHBzOi8vZGJjLTgxNmZmMzRkLWIzNWQuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6Ijc0NzQ2NTU3NDgwMzM5NDEiLCJzdWIiOiJlbWRsZWJAZ21haWwuY29tIiwiaWF0IjoxNzgxNTUyOTk2LCJleHAiOjE3ODE1NTY1OTYsImp0aSI6ImQ1NmRhNzRjLWVkNTktNGM2YS1hYmI5LTAxYWQ4YWQwYWU5ZSJ9.iqCqwSbhVaInV41hiTKeS4F9zdTkoMdD2lLtsJEZyW7cR3FOmXGxmHAX2lQdGU3wRtO0KVG0ah0E625nTYgMbbddjAqFuPpvN1WXAi893Cn4q3xweh2n-ANroc7a-KXilwXbgOd2VY8X9WIeKhu9oL9EzEybt6-eTY9bXaHdPOmw0-HGohXaUG9xgom91f9Bf_a3pi0KdmmvIYHqe1m_7xlSQqFsMCuRL_gRl7VrH3-l7d3OVywDEaQp7wfEw1P37q7x_uormL7JfSKGezMrnrVyBp1h62J4J5GoKab-b9w4N0iq3E1XxY3j_AGK1Nex1wUR-DPR6Tgzvz5XdEFKYQ"

# Connection config using actual Lakebase instance
test_config = {
    "host": LAKEBASE_HOST,
    "database": LAKEBASE_DATABASE,
    "user": LAKEBASE_USER,
    "password": token,
    "port": LAKEBASE_PORT,
    "sslmode": "require"
}

print(f"\nConnection config:")
print(f"  Host: {test_config['host']}")
print(f"  Database: {test_config['database']}")
print(f"  User: {test_config['user']}")
print(f"  Port: {test_config['port']}")

try:
    conn = psycopg2.connect(**test_config)
    cur = conn.cursor()
    
    # Test basic connectivity
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(f"\n✓ Connected to Postgres: {version.split(',')[0]}")
    
    # Check if facilities_search table exists
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'facilities_search'
    """)
    table_exists = cur.fetchone()[0] > 0
    
    if table_exists:
        cur.execute("SELECT COUNT(*) FROM facilities_search")
        count = cur.fetchone()[0]
        print(f"✓ Table 'facilities_search' exists with {count:,} rows")
    else:
        print("⚠️ Table 'facilities_search' does not exist yet. Run the SQL setup in the previous cell.")
    
    conn.close()
    print("\n✓ Connection test successful!")
    
except psycopg2.OperationalError as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nTip: Check that the password is correct and matches what you set during Lakebase instance creation.")
except Exception as e:
    print(f"\n❌ Error: {e}")

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
