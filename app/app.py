import os
import streamlit as st
import sys
import psycopg2
import pandas as pd

# Page config
st.set_page_config(
    page_title="Healthcare Facility Finder",
    page_icon="🏥",
    layout="wide"
)

# Lakebase connection configuration
LAKEBASE_CONFIG = {
    "host": "ep-misty-forest-d8hvkz5k.database.us-east-2.cloud.databricks.com",
    "database": "databricks_postgres",
    "user": "emdleb@gmail.com",
    "password": os.getenv("DATABRICKS_TOKEN", ""),  # Read from environment variable
    "port": "5432",
    "sslmode": "require"
}

@st.cache_resource
def get_db_connection():
    """Create and cache database connection"""
    # Check if token is configured
    if not LAKEBASE_CONFIG["password"]:
        st.error("""
        ⚠️ Database token not configured. 
        
        Please set the DATABRICKS_TOKEN environment variable in the app configuration.
        You can get the token from the gold curation notebook (cell 9).
        """)
        return None
    
    try:
        conn = psycopg2.connect(**LAKEBASE_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def search_facilities(search_term="", state="", city="", limit=100):
    """Search facilities from Lakebase"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Build query
        query = "SELECT * FROM facilities_search WHERE 1=1"
        params = []
        
        if search_term:
            query += " AND name ILIKE %s"
            params.append(f"%{search_term}%")
        
        if state:
            query += " AND address_stateorregion = %s"
            params.append(state)
        
        if city:
            query += " AND address_city ILIKE %s"
            params.append(f"%{city}%")
        
        query += f" ORDER BY name LIMIT {limit}"
        
        # Execute query
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def get_stats():
    """Get dashboard statistics"""
    conn = get_db_connection()
    if not conn:
        return {"total": 0, "states": 0, "cities": 0, "with_gps": 0}
    
    try:
        cursor = conn.cursor()
        
        # Total facilities
        cursor.execute("SELECT COUNT(*) FROM facilities_search")
        total = cursor.fetchone()[0]
        
        # Unique states
        cursor.execute("SELECT COUNT(DISTINCT address_stateorregion) FROM facilities_search WHERE address_stateorregion IS NOT NULL")
        states = cursor.fetchone()[0]
        
        # Unique cities
        cursor.execute("SELECT COUNT(DISTINCT address_city) FROM facilities_search WHERE address_city IS NOT NULL")
        cities = cursor.fetchone()[0]
        
        # With GPS coordinates
        cursor.execute("SELECT COUNT(*) FROM facilities_search WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        with_gps = cursor.fetchone()[0]
        
        cursor.close()
        
        return {"total": total, "states": states, "cities": cities, "with_gps": with_gps}
    except Exception as e:
        st.error(f"Stats error: {e}")
        return {"total": 0, "states": 0, "cities": 0, "with_gps": 0}

def get_states():
    """Get list of states for dropdown"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT address_stateorregion FROM facilities_search WHERE address_stateorregion IS NOT NULL ORDER BY address_stateorregion")
        states = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return states
    except:
        return []

# Main content
st.title("🏥 Healthcare Facility Finder")
st.markdown("### Data-AVengers Team | DAIS 2026 Virtue Foundation Dataset")

# Get and display stats
stats = get_stats()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Facilities", f"{stats['total']:,}")
with col2:
    st.metric("States Covered", stats['states'])
with col3:
    st.metric("Cities", f"{stats['cities']:,}")
with col4:
    st.metric("With GPS Coordinates", f"{stats['with_gps']:,}")

st.markdown("---")

# Search interface
st.subheader("🔍 Search Healthcare Facilities")

col1, col2, col3 = st.columns(3)

with col1:
    search_term = st.text_input("Facility Name", placeholder="e.g., Hospital, Clinic")

with col2:
    states = ["All States"] + get_states()
    selected_state = st.selectbox("State", states)
    state_filter = None if selected_state == "All States" else selected_state

with col3:
    city = st.text_input("City", placeholder="e.g., Mumbai, Delhi")

if st.button("🔎 Search", type="primary"):
    with st.spinner("Searching facilities..."):
        results = search_facilities(search_term, state_filter, city)
        
        if not results.empty:
            st.success(f"Found {len(results)} facilities")
            
            # Display results
            st.dataframe(
                results[['name', 'address_city', 'address_stateorregion', 'phone_numbers', 'email', 'facilityTypeId']],
                use_container_width=True,
                hide_index=True
            )
            
            # Show facilities with GPS on map
            with_gps = results[results['latitude'].notna() & results['longitude'].notna()]
            if not with_gps.empty:
                st.subheader(f"📍 Map View ({len(with_gps)} facilities)")
                st.map(with_gps[['latitude', 'longitude']], size=20, color="#ff0000")
        else:
            st.warning("No facilities found matching your search criteria.")

# Project info
with st.expander("📊 About This Project"):
    st.markdown("""
    **Healthcare Facility Finder** is built on a medallion architecture:
    
    * **Bronze Layer**: Raw data ingestion (176,421 rows)
    * **Silver Layer**: Data cleaning & standardization  
    * **Gold Layer**: Search-optimized view in Lakebase Postgres
    
    **Tech Stack:**
    - Databricks Unity Catalog
    - Lakebase (Postgres) for <10ms queries
    - Streamlit
    - Python + SQL
    """)

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
<strong>Healthcare Facility Finder v1.0</strong><br>
Built with Databricks • Lakebase • Streamlit<br>
Team: Data-AVengers | DAIS 2026 Hackathon
</div>
""", unsafe_allow_html=True)
