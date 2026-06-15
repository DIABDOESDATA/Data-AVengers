# Healthcare Facility Finder

A comprehensive healthcare facility locator application built with Databricks medallion architecture, Lakebase for sub-10ms searches, and a modern Streamlit web interface.

## 📊 Data Source

**Databricks Virtue Foundation Dataset (DAIS 2026)**
- Catalog: `databricks_virtue_foundation_dataset_dais_2026`
- Schema: `virtue_foundation_dataset`
- Tables:
  - `facilities` (10,088 rows) - Healthcare facilities with location, services, specialties
  - `india_post_pincode_directory` - Geographic pincode lookup
  - `nfhs_5_district_health_indicators` - District health statistics

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────┐
│  Marketplace    │
│  Source Data    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  Raw data ingestion
│  (UC Tables)    │  - facilities_bronze
└────────┬────────┘  - pincodes_bronze
         │           - health_indicators_bronze
         ▼
┌─────────────────┐
│  Silver Layer   │  Cleaned & standardized
│  (UC Tables)    │  - facilities_silver
└────────┬────────┘  - pincodes_silver
         │           - health_indicators_silver
         ▼
┌─────────────────┐
│  Gold Layer     │  Search-optimized & curated
│  (UC Tables)    │  - facilities_search_gold
└────────┬────────┘
         │
         │  Continuous Sync
         ▼
┌─────────────────┐
│  Lakebase PG    │  Sub-10ms reads
│  (Postgres)     │  - facilities_search
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Databricks App  │  Streamlit UI
│  (Python)       │  - Facility search
└─────────────────┘  - Interactive map
                     - Service filtering
```

## 📁 Project Structure

```
Data-AVengers/
├── notebooks/
│   ├── 00_setup_and_config.py          # UC schema + Lakebase setup
│   ├── 01_bronze_ingestion.py          # Raw data ingestion
│   ├── 02_silver_transformation.py     # Data cleaning
│   ├── 03_gold_curation.py             # Search optimization + Lakebase sync
│   └── 04_deploy_app.py                # Deploy Streamlit app
│
├── app/
│   ├── app.py                          # Streamlit facility finder UI
│   ├── app.yaml                        # App configuration
│   └── requirements.txt                # Python dependencies
│
└── README.md                           # This file
```

## 🚀 Quick Start

### 1. Run Setup

Open and run: `notebooks/00_setup_and_config.py`
- Creates Unity Catalog schema: `main.healthcare_facility_finder`
- Provides Lakebase instance setup instructions

### 2. Build Bronze Layer

Run: `notebooks/01_bronze_ingestion.py`
- Ingests raw data from marketplace catalog
- Creates bronze tables with full schema

### 3. Build Silver Layer

Run: `notebooks/02_silver_transformation.py`
- Cleans and standardizes data
- Handles nulls, data types, and validation

### 4. Build Gold Layer & Sync to Lakebase

Run: `notebooks/03_gold_curation.py`
- Creates search-optimized views
- Sets up continuous sync to Lakebase
- Configures indexes for fast queries

### 5. Deploy Application

Run: `notebooks/04_deploy_app.py`
- Deploys Streamlit app to Databricks Apps
- Configures app service principal permissions
- Provides app URL for testing

## 🎯 Key Features

### Application Features
- **Smart Search**: Find facilities by name, location, services, specialties
- **Geographic Filters**: Search by state, district, pincode
- **Service Categories**: Filter by facility type, operator, specialties
- **Contact Information**: Phone, email, website, address
- **Real-time Data**: Powered by Lakebase with sub-10ms response times

### Technical Features
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Unity Catalog**: Governed, versioned tables
- **Lakebase Sync**: Continuous replication from UC to Postgres
- **Sub-10ms Queries**: Lakebase-backed facility search
- **Databricks Apps**: Secure, serverless app hosting
- **Modern UI**: Streamlit with custom styling

## 📝 Configuration

Edit configuration in `notebooks/00_setup_and_config.py`:

```python
SOURCE_CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
SOURCE_SCHEMA = "virtue_foundation_dataset"
TARGET_CATALOG = "main"
TARGET_SCHEMA = "healthcare_facility_finder"
APP_NAME = "healthcare_facility_finder"
```

## 🔒 Security & Permissions

The deployment notebook (`04_deploy_app.py`) automatically:
- Creates app service principal
- Grants SELECT on all gold tables
- Grants Lakebase connection permissions
- Configures secure app access

## 🛠️ Development

### Requirements
- Databricks workspace (AWS/Azure/GCP)
- Unity Catalog enabled
- Lakebase instance provisioned
- SQL Warehouse for notebook execution

### Deployment
```bash
# All deployment handled via notebooks
# No local CLI required - pure Databricks-native
```

## 📊 Data Quality

Each layer includes data quality checks:
- **Bronze**: Schema validation, row counts
- **Silver**: Null handling, data type validation
- **Gold**: Business rule validation, search optimization

## 🤝 Team: Data-AVengers

Built for Databricks Hackathon with the Virtue Foundation dataset.

## 📄 License

This project uses the Databricks Virtue Foundation Dataset (DAIS 2026) under marketplace terms.