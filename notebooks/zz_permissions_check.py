# Databricks notebook source
# Run this notebook in your Databricks workspace to collect permission diagnostics.

# 1) Who am I and active catalog/schema
# Run this cell (SQL):
# %sql
# SELECT current_user(), current_catalog(), current_schema();

# 2) Show grants on catalog
# %sql
# SHOW GRANTS ON CATALOG main;

# 3) Show grants on schema
# %sql
# SHOW GRANTS ON SCHEMA main.healthcare_facility_finder;

# 4) Show grants on a key table
# %sql
# SHOW GRANTS ON TABLE main.healthcare_facility_finder.facilities_search_gold;

# Instructions:
#  - Open this notebook in Databricks and run each cell.
#  - Paste the outputs here and I'll interpret them and produce exact grant commands if needed.
#  - If you prefer, run all SQL commands in a SQL editor and paste results.
