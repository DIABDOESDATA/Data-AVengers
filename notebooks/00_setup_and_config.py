# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC -- Healthcare Facility Finder - Grant Unity Catalog Permissions
# MAGIC -- Service Principal: app-50zebz healthcare-facility-finder
# MAGIC -- Note: Use service principal CLIENT ID instead of name to avoid space issues
# MAGIC
# MAGIC -- The service principal client ID is: 27947e33-cbdf-4e1f-a832-fcf68551eda7
# MAGIC -- This is more reliable than using the display name with spaces
# MAGIC
# MAGIC -- Grant USE CATALOG permission
# MAGIC GRANT USE CATALOG ON CATALOG `main` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC
# MAGIC -- Grant USE SCHEMA permission
# MAGIC GRANT USE SCHEMA ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC
# MAGIC -- Grant SELECT on entire schema (includes all tables)
# MAGIC GRANT SELECT ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC
# MAGIC -- Specifically grant SELECT on the gold view
# MAGIC GRANT SELECT ON TABLE `main`.`healthcare_facility_finder`.`facilities_search_gold` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant permissions to the app service principal
# MAGIC GRANT USE CATALOG ON CATALOG `main` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT USE SCHEMA ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT SELECT ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG `main` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT USE SCHEMA ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT SELECT ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant Unity Catalog permissions to the app service principal
# MAGIC GRANT USE CATALOG ON CATALOG `main` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT USE SCHEMA ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;
# MAGIC GRANT SELECT ON SCHEMA `main`.`healthcare_facility_finder` TO `27947e33-cbdf-4e1f-a832-fcf68551eda7`;

# COMMAND ----------

# DBTITLE 1,Cell 5
import psycopg2

# Lakebase connection
token = "eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBWWdyR190Skh5RUE9PSIsImlzcyI6Imh0dHBzOi8vZGJjLTgxNmZmMzRkLWIzNWQuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6Ijc0NzQ2NTU3NDgwMzM5NDEiLCJzdWIiOiJlbWRsZWJAZ21haWwuY29tIiwiaWF0IjoxNzgxNTU4MTEwLCJleHAiOjE3ODE1NjE3MTAsImp0aSI6IjA2ODA3MWNhLTI5NzMtNDRkYS1iZWViLTZlMjQzMmRmMDNjMCJ9.kkMfx6jPR0J-oxQvDh1PuoeyYKYw2Ypz4SLs_TqYAXUBK7DIVowMqiP7m2sEWVvgC5LLjRSK9UwYWxxQvEdZ2uGfYUXCVxHYDBkfZHOwSO2mXOhFZShg7Id5qzehtLtduoj3Vvn3slhvL72c8gtKyqAmWsVzWK-8fd1Z7zhxrETYl1H-S8V3iru-0vlSLTWvSWIiO8wUCAyxISNGOm-2jl-NwI_l73v5gyknvVyMAJ9CoOo__UqX-PUw4TKUaNaaKwrUK-Jzw9f8XgqCF5_amzVjob_3n_RMK9F2rfvXr0zQ8N0VjDgNDdnWyLmi_RR7F2-zHQa8GUh-l5l4SOX48g"

conn = psycopg2.connect(
    host="ep-misty-forest-d8hvkz5k.database.us-east-2.cloud.databricks.com",
    database="databricks_postgres",
    user="emdleb@gmail.com",
    password=token,
    port=5432,
    sslmode="require"
)

cur = conn.cursor()

app_sp_client_id = "064b78a4-b14a-4a5d-93f8-3a3213d371c9"

print("Granting Lakebase permissions to app service principal...\n")

try:
    # Grant CONNECT
    cur.execute(f"GRANT CONNECT ON DATABASE databricks_postgres TO \"{app_sp_client_id}\"")
    print("✓ Granted CONNECT on database")
    
    # Grant USAGE on schema
    cur.execute(f"GRANT USAGE ON SCHEMA public TO \"{app_sp_client_id}\"")
    print("✓ Granted USAGE on schema public")
    
    # Grant SELECT on table
    cur.execute(f"GRANT SELECT ON TABLE facilities_search TO \"{app_sp_client_id}\"")
    print("✓ Granted SELECT on table facilities_search")
    
    conn.commit()
    print("\n✓ All permissions granted successfully!")
    
except Exception as e:
    conn.rollback()
    print(f"❌ Error: {e}")
    
finally:
    cur.close()
    conn.close()

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT CONNECT ON DATABASE databricks_postgres TO "064b78a4-b14a-4a5d-93f8-3a3213d371c9";
# MAGIC GRANT USAGE ON SCHEMA public TO "064b78a4-b14a-4a5d-93f8-3a3213d371c9";
# MAGIC GRANT SELECT ON TABLE facilities_search TO "064b78a4-b14a-4a5d-93f8-3a3213d371c9";
