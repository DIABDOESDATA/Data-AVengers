import os
import streamlit as st
import sys

# Page config
st.set_page_config(
    page_title="Healthcare Facility Finder",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Healthcare Facility Finder")
st.markdown("### Data-AVengers Team | DAIS 2026 Virtue Foundation Dataset")

st.success("✅ App is loading successfully!")

st.info("""
**Good news!** The Streamlit app infrastructure is working.

We're currently fixing the database connection to Unity Catalog.
The app will be fully functional once the connection is configured properly.
""")

st.subheader("📊 Project Overview")

st.markdown("""
**Healthcare Facility Finder** is a Streamlit application that provides:

* 🔍 **Search**: Find healthcare facilities by name, state, or city
* 📍 **Interactive Map**: Visualize facilities across India
* 📊 **Dashboard Metrics**: 
  - 10,023 total facilities
  - 37 states covered
  - 1,000+ cities
  - 9,964 facilities with GPS coordinates

**Data Pipeline:**
- **Bronze Layer**: Raw data ingestion (176,421 rows)
- **Silver Layer**: Data cleaning & standardization
- **Gold Layer**: Search-optimized view (`main.healthcare_facility_finder.facilities_search_gold`)

**Tech Stack:**
- Databricks Unity Catalog
- Streamlit
- Folium (Interactive maps)
- Python + SQL
""")

st.subheader("🔧 Diagnostic Information")

with st.expander("View Environment Configuration", expanded=False):
    st.text(f"Python Version: {sys.version}")
    st.text(f"Streamlit working directory: {os.getcwd()}")
    st.text(f"Environment variables loaded: {len(os.environ)}")

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
<strong>Healthcare Facility Finder v1.0</strong><br>
Built with Databricks • Unity Catalog • Streamlit<br>
Team: Data-AVengers
</div>
""", unsafe_allow_html=True)

st.markdown('''
<style>
:root {
  --primary-color: #FF3621;
  --secondary-color: #0B2026;
  --background-color: #F9F7F4;
  --card-background: #FFFFFF;
  --text-color: #0B2026;
  --border-color: #EEEDE9;
}

body {
    background-color: var(--background-color);
    color: var(--text-color);
}

.main-header {
    background: linear-gradient(135deg, #FF3621 0%, #0B2026 100%);
    color: white;
    padding: 2rem;
    border-radius: 12px;
    margin-bottom: 2rem;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
}

.main-header h1 {
    color: white;
    margin: 0;
    font-size: 2.5rem;
    font-weight: 700;
}

.main-header p {
    color: rgba(255,255,255,0.9);
    margin: 0.5rem 0 0 0;
    font-size: 1.1rem;
}

.facility-card {
    background: var(--card-background);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 1.5rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    transition: all 0.3s ease;
}

.facility-card:hover {
    box-shadow: 0 6px 12px rgba(0,0,0,0.1);
    transform: translateY(-2px);
}

.facility-name {
    color: var(--primary-color);
    font-size: 1.4rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
}

.facility-type {
    display: inline-block;
    background: #FF362120;
    color: var(--primary-color);
    padding: 0.25rem 0.75rem;
    border-radius: 20px;
    font-size: 0.85rem;
    font-weight: 600;
    margin-right: 0.5rem;
    margin-bottom: 0.5rem;
}

.contact-info {
    color: var(--secondary-color);
    font-size: 0.95rem;
    margin: 0.5rem 0;
}

.contact-info strong {
    color: var(--primary-color);
}

.metric-card {
    background: var(--card-background);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    padding: 1rem;
    text-align: center;
}

.metric-value {
    color: var(--primary-color);
    font-size: 2rem;
    font-weight: 700;
}

.metric-label {
    color: var(--secondary-color);
    font-size: 0.9rem;
    margin-top: 0.5rem;
}

.stButton>button {
    background: linear-gradient(135deg, #FF3621 0%, #0B2026 100%);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 0.75rem 2rem;
    font-weight: 600;
    transition: all 0.3s ease;
}

.stButton>button:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(255,54,33,0.3);
}

.search-filters {
    background: var(--card-background);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 1.5rem;
    margin-bottom: 2rem;
}

.footer {
    text-align: center;
    color: var(--secondary-color);
    padding: 2rem 0 1rem 0;
    border-top: 1px solid var(--border-color);
    margin-top: 3rem;
    font-size: 0.9rem;
}
</style>
''', unsafe_allow_html=True)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource(show_spinner=False)
def get_db_connection():
    """Create a connection to Lakebase Postgres"""
    try:
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            port=LAKEBASE_PORT,
            database=LAKEBASE_DATABASE,
            user=LAKEBASE_USER,
            password=LAKEBASE_PASSWORD
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        st.info("Please ensure Lakebase is configured and the app is deployed with correct credentials.")
        return None

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def load_facilities(search_name=None, state_filter=None, city_filter=None, facility_types=None, limit=100):
    """Load facilities from Unity Catalog with optional filters"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.facilities_search_gold WHERE 1=1"
        
        if search_name:
            search_clean = search_name.replace("'", "''")
            query += f" AND name LIKE '%{search_clean}%'"
        
        if state_filter:
            query += f" AND address_stateOrRegion = '{state_filter}'"
        
        if city_filter:
            city_clean = city_filter.replace("'", "''")
            query += f" AND address_city LIKE '%{city_clean}%'"
        
        query += f" ORDER BY name LIMIT {limit}"
        
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=columns)
        
        cursor.close()
        conn.close()
        return df
    
    except Exception as e:
        st.error(f"Error loading facilities: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_unique_states():
    """Get list of unique states from facilities"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT address_stateorregion FROM facilities_search WHERE address_stateorregion IS NOT NULL ORDER BY address_stateorregion")
            states = [row[0] for row in cur.fetchall()]
        conn.close()
        return states
    except:
        return []

@st.cache_data(ttl=600, show_spinner=False)
def get_facility_stats():
    """Get summary statistics about facilities"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_facilities,
                COUNT(DISTINCT address_stateOrRegion) as total_states,
                COUNT(DISTINCT address_city) as total_cities,
                SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as facilities_with_location
            FROM {UC_CATALOG}.{UC_SCHEMA}.facilities_search_gold
        """)
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        stats = dict(zip(columns, result)) if result else {}
        cursor.close()
        conn.close()
        return stats
    except Exception as e:
        st.error(f"Error loading stats: {str(e)}")
        return {}

# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_header():
    """Render the application header"""
    st.markdown('''
        <div class="main-header">
            <h1>🏥 Healthcare Facility Finder</h1>
            <p>Search and discover healthcare facilities across India using the Virtue Foundation dataset</p>
        </div>
    ''', unsafe_allow_html=True)

def render_stats_dashboard():
    """Render key metrics dashboard"""
    stats = get_facility_stats()
    
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{stats.get('total_facilities', 0):,}</div>
                    <div class="metric-label">Total Facilities</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col2:
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{stats.get('total_states', 0)}</div>
                    <div class="metric-label">States Covered</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col3:
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{stats.get('total_cities', 0):,}</div>
                    <div class="metric-label">Cities</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with col4:
            st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-value">{stats.get('facilities_with_location', 0):,}</div>
                    <div class="metric-label">Mapped Locations</div>
                </div>
            ''', unsafe_allow_html=True)

def render_facility_card(facility):
    """Render a single facility card"""
    st.markdown(f'''
        <div class="facility-card">
            <div class="facility-name">{facility.get('name', 'Unknown Facility')}</div>
            <div>
                <span class="facility-type">{facility.get('organizationType', 'Healthcare')}</span>
                {f'<span class="facility-type">📍 {facility.get("address_city", "")}</span>' if facility.get('address_city') else ''}
            </div>
            <div class="contact-info">
                <strong>📍 Address:</strong> {facility.get('address_line1', 'N/A')}, {facility.get('address_city', '')}, {facility.get('address_stateOrRegion', '')} {facility.get('address_zipOrPostcode', '')}
            </div>
            {f'<div class="contact-info"><strong>📞 Phone:</strong> {facility.get("phoneNumbers", "N/A")}</div>' if facility.get('phoneNumbers') else ''}
            {f'<div class="contact-info"><strong>🌐 Website:</strong> <a href="{facility.get("websites", "")}" target="_blank">{facility.get("websites", "")}</a></div>' if facility.get('websites') else ''}
            {f'<div class="contact-info"><strong>📧 Email:</strong> {facility.get("email", "N/A")}</div>' if facility.get('email') else ''}
        </div>
    ''', unsafe_allow_html=True)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    render_header()
    
    # Show diagnostic info
    with st.expander("🔧 Diagnostic Info", expanded=False):
        st.info(f"Hostname: {DATABRICKS_SERVER_HOSTNAME}")
        st.info(f"HTTP Path: {DATABRICKS_HTTP_PATH}")
        st.info(f"Catalog: {UC_CATALOG}.{UC_SCHEMA}")
        
        # Test connection
        test_conn = get_db_connection()
        if test_conn:
            st.success("✅ Database connection successful!")
            test_conn.close()
        else:
            st.error("❌ Database connection failed")
    
    render_stats_dashboard()
    
    st.markdown("---")
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Search Filters")
        
        search_name = st.text_input("🏥 Facility Name", placeholder="Search by name...")
        
        states = get_unique_states()
        state_filter = st.selectbox("📍 State", ["All States"] + states)
        state_filter = None if state_filter == "All States" else state_filter
        
        city_filter = st.text_input("🏙️ City", placeholder="Enter city name...")
        city_filter = None if not city_filter else city_filter
        
        max_results = st.slider("Max Results", 10, 500, 100, 10)
        
        search_clicked = st.button("🔍 Search", use_container_width=True)
    
    # Main content area
    if search_clicked or search_name or state_filter or city_filter:
        with st.spinner("Searching facilities..."):
            facilities_df = load_facilities(
                search_name=search_name or None,
                state_filter=state_filter,
                city_filter=city_filter,
                limit=max_results
            )
        
        if not facilities_df.empty:
            st.success(f"✅ Found {len(facilities_df)} facilities")
            
            # Show map if facilities have coordinates
            if 'latitude' in facilities_df.columns and 'longitude' in facilities_df.columns:
                facilities_with_coords = facilities_df.dropna(subset=['latitude', 'longitude'])
                
                if len(facilities_with_coords) > 0:
                    st.subheader("📍 Facility Locations")
                    
                    # Create map centered on India
                    m = folium.Map(location=[20.5937, 78.9629], zoom_start=5)
                    
                    for _, facility in facilities_with_coords.iterrows():
                        folium.Marker(
                            location=[facility['latitude'], facility['longitude']],
                            popup=f"<b>{facility.get('name', 'Unknown')}</b><br>{facility.get('address_city', '')}",
                            tooltip=facility.get('name', 'Unknown'),
                            icon=folium.Icon(color='red', icon='hospital', prefix='fa')
                        ).add_to(m)
                    
                    st_folium(m, width=700, height=500)
            
            # Show facility cards
            st.subheader(f"🏥 Facilities ({len(facilities_df)} results)")
            
            for _, facility in facilities_df.iterrows():
                render_facility_card(facility)
        else:
            st.warning("No facilities found matching your criteria. Try adjusting your filters.")
    else:
        st.info("👈 Use the sidebar filters to search for healthcare facilities")
        
        # Show sample facilities
        sample_df = load_facilities(limit=10)
        if not sample_df.empty:
            st.subheader("📋 Sample Facilities")
            for _, facility in sample_df.head(5).iterrows():
                render_facility_card(facility)
    
    # Footer
    st.markdown('''
        <div class="footer">
            <strong>Healthcare Facility Finder v1.0</strong> | Built with Databricks • Unity Catalog • Lakebase • Streamlit<br>
            Data source: Databricks Virtue Foundation Dataset (DAIS 2026) | Team: Data-AVengers
        </div>
    ''', unsafe_allow_html=True)

if __name__ == "__main__":
    main()