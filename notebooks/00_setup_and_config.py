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
