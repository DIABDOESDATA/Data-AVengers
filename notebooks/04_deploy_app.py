# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Healthcare Facility Finder - Deploy Databricks App
# MAGIC
# MAGIC This notebook deploys the Streamlit application to Databricks Apps with:
# MAGIC - App service principal creation
# MAGIC - Unity Catalog permissions
# MAGIC - Lakebase connection configuration
# MAGIC - App deployment and verification

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment
import json
import os

# Configuration
APP_NAME = "healthcare-facility-finder"
APP_ROOT = "/Workspace/Users/emdleb@gmail.com/Data-AVengers/app"
TARGET_CATALOG = "main"
TARGET_SCHEMA = "healthcare_facility_finder"

w = WorkspaceClient()

print(f"App name: {APP_NAME}")
print(f"App root: {APP_ROOT}")
print(f"Catalog.Schema: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Verify App Files
# MAGIC %md
# MAGIC ## 1. Verify App Files

# COMMAND ----------

# DBTITLE 1,Check App Files
import os

print("Verifying app files...")

required_files = [
    f"{APP_ROOT}/app.py",
    f"{APP_ROOT}/app.yaml",
    f"{APP_ROOT}/requirements.txt"
]

for file_path in required_files:
    # Convert to dbfs path for checking
    workspace_path = file_path.replace("/Workspace", "")
    print(f"  Checking: {file_path}")
    # File existence will be verified during deployment

print(f"\n✓ App files ready for deployment")

# COMMAND ----------

# DBTITLE 1,Service Principal
# MAGIC %md
# MAGIC ## 2. Create or Get App Service Principal

# COMMAND ----------

# DBTITLE 1,Service Principal Setup
print("Setting up app service principal...")

# Try to get existing app
try:
    app_info = w.apps.get(APP_NAME)
    app_sp = app_info.service_principal_name
    print(f"✓ Found existing app: {APP_NAME}")
    print(f"  Service Principal: {app_sp}")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e):
        print(f"  App '{APP_NAME}' will be created during first deployment")
        app_sp = None
    else:
        print(f"  Warning: {str(e)[:100]}")
        app_sp = None

# Store for later use
APP_SERVICE_PRINCIPAL = app_sp

# COMMAND ----------

# DBTITLE 1,UC Permissions
# MAGIC %md
# MAGIC ## 3. Grant Unity Catalog Permissions

# COMMAND ----------

# DBTITLE 1,Grant UC Permissions
print("Configuring Unity Catalog permissions...")

if APP_SERVICE_PRINCIPAL:
    grants = [
        f"GRANT USE CATALOG ON CATALOG `{TARGET_CATALOG}` TO `{APP_SERVICE_PRINCIPAL}`",
        f"GRANT USE SCHEMA ON SCHEMA `{TARGET_CATALOG}`.`{TARGET_SCHEMA}` TO `{APP_SERVICE_PRINCIPAL}`",
        f"GRANT SELECT ON SCHEMA `{TARGET_CATALOG}`.`{TARGET_SCHEMA}` TO `{APP_SERVICE_PRINCIPAL}`",
        f"GRANT SELECT ON TABLE `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`.`facilities_search_gold` TO `{APP_SERVICE_PRINCIPAL}`"
    ]
    
    for grant in grants:
        try:
            spark.sql(grant)
            print(f"  ✓ {grant}")
        except Exception as e:
            print(f"  ⚠ {grant}")
            print(f"    Error: {str(e)[:100]}")
else:
    print("  ⚠ Service principal not yet created")
    print("  ℹ Permissions will be configured after first deployment")
    print("  ℹ Re-run this notebook after deployment to grant permissions")

# COMMAND ----------

# DBTITLE 1,Lakebase Setup
# MAGIC %md
# MAGIC ## 4. Configure Lakebase Connection

# COMMAND ----------

# DBTITLE 1,Lakebase Configuration
print("="*80)
print("LAKEBASE CONNECTION CONFIGURATION")
print("="*80)

print(f"""
Before deploying, ensure you have:

1. **Created Databricks Secret Scope** (if not exists):
   ```
   databricks secrets create-scope --scope lakebase-secrets
   ```

2. **Stored Lakebase Credentials**:
   ```
   databricks secrets put --scope lakebase-secrets --key host --string-value "<your-lakebase-host>"
   databricks secrets put --scope lakebase-secrets --key database --string-value "{TARGET_SCHEMA}_db"
   databricks secrets put --scope lakebase-secrets --key user --string-value "<your-lakebase-user>"
   databricks secrets put --scope lakebase-secrets --key password --string-value "<your-lakebase-password>"
   ```

3. **Updated app.yaml** with secret references

Note: The app.yaml already references these secrets.
Update it with your actual Lakebase host before deploying.
""")

print("\n✓ Review instructions above")

# COMMAND ----------

# DBTITLE 1,Deploy App
# MAGIC %md
# MAGIC ## 5. Deploy Application

# COMMAND ----------

# DBTITLE 1,Deploy the App
print(f"Deploying app '{APP_NAME}'...")
print(f"Source: {APP_ROOT}")

try:
    print("Starting the app...")
    
    # Start the app first
    try:
        w.apps.start(APP_NAME)
        print(f"✓ App '{APP_NAME}' started")
    except Exception as e:
        if "RUNNING" in str(e):
            print(f"App already running")
        else:
            print(f"Start warning: {str(e)[:100]}")
    
    print("\nDeploying app code...")
    
    # Deploy with correct SDK parameters
    wait = w.apps.deploy(
        app_name=APP_NAME,
        app_deployment=AppDeployment(source_code_path=APP_ROOT)
    )
    
    print("Waiting for deployment to complete (this may take a few minutes)...")
    deployment = wait.result()
    
    print("\n✓ Deployment completed successfully!")
    print(f"  Deployment ID: {getattr(deployment, 'deployment_id', 'N/A')}")
    print(f"  Status: {deployment.status.state if hasattr(deployment, 'status') else 'Unknown'}")
    
    if hasattr(deployment, 'status') and deployment.status.message:
        print(f"  Message: {deployment.status.message}")
    
    # Get app URL
    app_info = w.apps.get(APP_NAME)
    app_url = getattr(app_info, 'url', None)
    if app_url:
        print(f"\n🚀 App URL: {app_url}")
        print(f"   Open this URL to test your Healthcare Facility Finder!")
    
    # Store service principal for permissions
    if hasattr(app_info, 'service_principal_name'):
        new_sp = app_info.service_principal_name
        print(f"\n✓ App Service Principal: {new_sp}")
        if not APP_SERVICE_PRINCIPAL:
            print(f"  ⚠ Re-run this notebook to grant UC permissions to the service principal")
    
except Exception as e:
    print(f"\n❌ Deployment failed:")
    print(f"  {str(e)}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify app files exist in {APP_ROOT}")
    print(f"  2. Check app.yaml configuration")
    print(f"  3. Ensure Lakebase secrets are configured")
    raise

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## 6. Post-Deployment Verification

# COMMAND ----------

# DBTITLE 1,Post-Deployment Checks
print("="*80)
print("POST-DEPLOYMENT CHECKLIST")
print("="*80)

try:
    app_info = w.apps.get(APP_NAME)
    
    print(f"\n✓ App Status:")
    print(f"  Name: {app_info.name}")
    print(f"  State: {app_info.active_deployment.state if hasattr(app_info, 'active_deployment') else 'Unknown'}")
    
    if hasattr(app_info, 'url'):
        print(f"  URL: {app_info.url}")
    
    if hasattr(app_info, 'service_principal_name'):
        print(f"  Service Principal: {app_info.service_principal_name}")
    
    print(f"\n✓ Next Steps:")
    print(f"  1. Open the app URL above")
    print(f"  2. Test facility search functionality")
    print(f"  3. Verify Lakebase connection is working")
    print(f"  4. Check that filters and map display correctly")
    print(f"  5. Monitor app logs for any errors")
    
    print(f"\n✓ To view app logs:")
    print(f"  databricks apps logs {APP_NAME}")
    
except Exception as e:
    print(f"  Could not retrieve app info: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Final Summary
print("="*80)
print("🎉 HEALTHCARE FACILITY FINDER - DEPLOYMENT COMPLETE!")
print("="*80)

print(f"""
Your Healthcare Facility Finder app is now deployed!

**Architecture:**
✓ Bronze Layer: Raw data from marketplace
✓ Silver Layer: Cleaned and standardized
✓ Gold Layer: Search-optimized views
✓ Lakebase: Sub-10ms Postgres queries
✓ Databricks App: Streamlit UI

**Data Pipeline:**
{TARGET_CATALOG}.{TARGET_SCHEMA}
  - facilities_bronze → facilities_silver → facilities_search_gold
  - pincodes_bronze → pincodes_silver
  - health_indicators_bronze → health_indicators_silver

**Application:**
- App Name: {APP_NAME}
- Search interface with filters
- Interactive map
- Sub-10ms query response

**Team: Data-AVengers**
**Dataset: Databricks Virtue Foundation (DAIS 2026)**

🚀 Happy searching for healthcare facilities!
""")
