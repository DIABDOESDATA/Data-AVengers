import json
import re
import streamlit as st
import psycopg2
import pandas as pd
import pydeck as pdk
from databricks.sdk import WorkspaceClient

st.set_page_config(
    page_title="Healthcare Facility Capabilities Verification",
    page_icon="🏥",
    layout="wide"
)

# Lakebase Autoscaling connection constants
LAKEBASE_HOST = "ep-misty-forest-d8hvkz5k.database.us-east-2.cloud.databricks.com"
LAKEBASE_DATABASE = "databricks_postgres"
LAKEBASE_ENDPOINT = "projects/healthcare-facility-finder/branches/production/endpoints/primary"

# ── Capability definitions ────────────────────────────────────────────────────

CAPABILITIES = ["ICU", "Emergency", "Maternity", "Oncology", "Trauma", "NICU"]

# Structured specialty keywords from the actual specialties JSON array
# Expanded to match more real-world specialty names from the database
SPECIALTY_KEYWORDS = {
    "ICU":       ["criticalCareMedicine", "criticalCare", "intensiveCare", "critical", "icu",
                  "anesthesia", "anesthesiology"],  # Anesthesia often indicates ICU capability
    "Emergency": ["emergencyMedicine", "pediatricEmergencyMedicine",
                  "emergencyPreparedness", "urgentCare", "emergency", "trauma"],
    "Maternity": ["gynecologyAndObstetrics", "maternalFetalMedicine",
                  "maternalFetal", "obstetrics", "gynecology", "obgyn",
                  "familyPlanningAndComplexContraception", "reproductiveEndocrinology",
                  "maternalHealth", "prenatal", "postnatal"],
    "Oncology":  ["medicalOncology", "surgicalOncology", "gynecologicalOncology",
                  "gynecologicOncology", "orthopedicOncology",
                  "radiationOncology", "oncology", "cancer", "hematology",
                  "neuroOncology", "pediatricOncology"],
    "Trauma":    ["burnAndTraumaPlasticSurgery", "traumaSurgery", "trauma",
                  "emergencyMedicine", "orthopedicSurgery", "neurosurgery",
                  "vascularSurgery", "cardiacSurgery", "generalSurgery"],
    "NICU":      ["neonatologyPerinatalMedicine", "neonatology",
                  "neonatalPerinatalMedicine", "neonatal", "nicu",
                  "pediatrics", "pediatricCardiology", "pediatricNeurology",
                  "pediatricSurgery"],
}

# Description text keywords (lowercase for matching)
DESC_KEYWORDS = {
    "ICU":       ["icu", "intensive care unit", "critical care unit"],
    "Emergency": ["emergency department", "emergency unit", "casualty", "a&e"],
    "Maternity": ["maternity", "labour ward", "labor ward", "obstetric", "antenatal"],
    "Oncology":  ["cancer", "oncology", "tumour", "tumor", "chemotherapy"],
    "Trauma":    ["trauma center", "trauma centre", "trauma care"],
    "NICU":      ["nicu", "neonatal icu", "neonatal intensive care"],
}

TRUSTED_FACILITY_TYPES = {"hospital", "clinic", "nursing_home"}

TRUST_COLORS = {
    "strong":  "#198754",
    "partial": "#ffc107",
    "weak":    "#fd7e14",
    "none":    "#ced4da",
}
TRUST_TEXT_COLORS = {
    "strong": "white",
    "partial": "#212529",
    "weak": "white",
    "none": "#6c757d",
}
TRUST_LABELS = {
    "strong": "Strong",
    "partial": "Partial",
    "weak": "Weak",
    "none": "—",
}
TRUST_ORDER = {"none": 0, "weak": 1, "partial": 2, "strong": 3}

SYSTEM_PROMPT = """You are a healthcare data analyst assistant for the Data-AVengers team, working with the DAIS 2026 Virtue Foundation Dataset — a database of 10,000+ healthcare facilities in India.

You help users assess facility trustworthiness using a TWO-FACTOR trust model:

**Factor 1: Capability Evidence** (from specialty documentation)
- Strong: Specialty confirmed by 2+ independent sources in structured data (high confidence)
- Partial: Listed once in structured specialty data from recognized facility type (moderate confidence)
- Weak: Only mentioned in free-text description OR single mention from non-standard facility type (low confidence)
- No Claim (—): Not documented anywhere

**Factor 2: Overall Facility Trust Score** (0-100, based on contact/location data completeness)
- Strong (70-100): Complete contact info, verified GPS, documented specialties, trusted operator
- Partial (40-69): Some missing data but core information present
- Weak (0-39): Significant gaps in contact info, location, or credibility indicators

**CRITICAL**: Both factors matter for trustworthiness assessment:
- A facility with "Strong" capability evidence BUT "Weak" overall trust → Claims are documented but facility data quality is poor (missing contact/location) → Trustworthiness is QUESTIONABLE
- A facility with "Weak" capability evidence AND "Weak" overall trust → Claims are poorly documented AND facility data is poor → Trustworthiness is LOW
- A facility with "Strong" capability evidence AND "Strong" overall trust → Well-documented claims from verified facility → Trustworthiness is HIGH

When assessing trustworthiness, consider BOTH the capability evidence strength AND the overall facility trust score. Be honest and direct in your assessment."""

# ── Database helpers ──────────────────────────────────────────────────────────

@st.cache_resource(ttl=3500)  # Refresh before the 1-hour OAuth token expiry
def get_db_connection():
    """Get a database connection. Returns a new connection each time to avoid 'connection closed' errors."""
    try:
        w = WorkspaceClient()
        user = w.current_user.me().user_name
        token = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            database=LAKEBASE_DATABASE,
            user=user,
            password=token,
            port=5432,
            sslmode="require",
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def get_fresh_connection():
    """Get a fresh database connection without caching."""
    try:
        w = WorkspaceClient()
        user = w.current_user.me().user_name
        token = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
        return psycopg2.connect(
            host=LAKEBASE_HOST,
            database=LAKEBASE_DATABASE,
            user=user,
            password=token,
            port=5432,
            sslmode="require",
        )
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def search_facilities(search_term="", state="", city="", limit=100):
    conn = get_fresh_connection()
    if not conn:
        return pd.DataFrame()
    try:
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
        result = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Query error: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

def get_facilities_for_evaluation(search_term="", state="", limit=200):
    conn = get_fresh_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = "SELECT * FROM facilities_search WHERE 1=1"
        params = []
        if search_term:
            query += " AND name ILIKE %s"
            params.append(f"%{search_term}%")
        if state:
            query += " AND address_stateorregion = %s"
            params.append(state)
        query += " ORDER BY name LIMIT %s"
        params.append(limit)
        result = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Query error: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

def get_stats():
    conn = get_fresh_connection()
    if not conn:
        return {"total": 0, "states": 0, "cities": 0, "with_gps": 0}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM facilities_search")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT address_stateorregion) FROM facilities_search WHERE address_stateorregion IS NOT NULL")
        states = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT address_city) FROM facilities_search WHERE address_city IS NOT NULL")
        cities = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM facilities_search WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        with_gps = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"total": total, "states": states, "cities": cities, "with_gps": with_gps}
    except Exception as e:
        st.error(f"Stats error: {e}")
        if conn:
            conn.close()
        return {"total": 0, "states": 0, "cities": 0, "with_gps": 0}

def get_states():
    conn = get_fresh_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT address_stateorregion FROM facilities_search "
            "WHERE address_stateorregion IS NOT NULL ORDER BY address_stateorregion"
        )
        states = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return states
    except Exception as e:
        if conn:
            conn.close()
        return []

@st.cache_data(ttl=600)
def get_facility_names():
    conn = get_fresh_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT name FROM facilities_search "
            "WHERE name IS NOT NULL ORDER BY name LIMIT 5000"
        )
        names = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return names
    except Exception:
        if conn:
            conn.close()
        return []

def search_and_evaluate(name="", state="", city="", limit=200):
    """Unified search returning all columns needed for display, trust, and map."""
    conn = get_fresh_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
            SELECT unique_id, name, address_city, address_stateorregion,
                   phone_numbers, email, websites, facilitytypeid,
                   specialties, description, latitude, longitude,
                   operatortypeid, address_line1, address_zipOrPostcode
            FROM facilities_search WHERE 1=1
        """
        params = []
        if name:
            query += " AND name ILIKE %s"
            params.append(f"%{name}%")
        if state:
            query += " AND address_stateorregion = %s"
            params.append(state)
        if city:
            query += " AND address_city ILIKE %s"
            params.append(f"%{city}%")
        query += " ORDER BY name LIMIT %s"
        params.append(limit)
        result = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Query error: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

def search_for_chat(search_dict, limit=10):
    """Search for facilities using name and/or state"""
    conn = get_fresh_connection()
    if not conn:
        return pd.DataFrame()
    try:
        sql_query = "SELECT unique_id, name, address_city, address_stateorregion, facilitytypeid, specialties, description FROM facilities_search WHERE 1=1"
        params = []
        
        # Handle dict or string input for backward compatibility
        if isinstance(search_dict, str):
            search_dict = {"name": search_dict, "state": None}
        
        name = search_dict.get("name")
        state = search_dict.get("state")
        
        if name:
            sql_query += " AND name ILIKE %s"
            params.append(f"%{name}%")
        
        if state:
            sql_query += " AND address_stateorregion = %s"
            params.append(state)
        
        sql_query += " ORDER BY name LIMIT %s"
        params.append(limit)
        
        result = pd.read_sql_query(sql_query, conn, params=params)
        conn.close()
        return result
    except Exception as e:
        if conn:
            conn.close()
        return pd.DataFrame()

def extract_search_terms(text):
    """Pull likely facility name keywords and location from user message."""
    skip = {"Can", "Which", "What", "Does", "Is", "Are", "Do", "How", "Tell",
            "Show", "Find", "This", "The", "That", "Please", "Give", "List", "Really", "With", "Have"}
    
    # Check if this is a location-based query (e.g., "facilities in Maharashtra")
    location_pattern = r'\b(?:in|from)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)\b'
    location_match = re.search(location_pattern, text)
    
    # Prefer quoted strings for facility names
    quoted = re.findall(r'"([^"]+)"', text)
    if quoted:
        return {"name": quoted[0], "state": location_match.group(1) if location_match else None}
    
    # Look for facility names - they often contain words like Hospital, Institute, Centre, Clinic
    facility_keywords = r'\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*(?:\s+(?:Hospital|Institute|Centre|Center|Clinic|Medical|Sciences|Technology|Care|Health))[a-zA-Z\s,]*)'
    facility_match = re.search(facility_keywords, text)
    if facility_match:
        # Clean up the match
        name = facility_match.group(1).strip()
        # Remove trailing commas and location info
        name = re.sub(r',.*$', '', name)
        return {"name": name, "state": location_match.group(1) if location_match else None}
    
    # If it's a location-only query, return just the state
    if location_match:
        return {"name": None, "state": location_match.group(1)}
    
    # Fall back to capitalized proper-noun runs (but take more words)
    caps = re.findall(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', text)
    meaningful = [c for c in caps if c not in skip]
    # Take up to 8 words to capture longer facility names
    name = " ".join(meaningful[:8]) if meaningful else None
    return {"name": name, "state": None}

def build_chat_context(df):
    """Turn a DataFrame of facilities into a text block for the LLM."""
    if df.empty:
        return None
    parts = []
    for _, row in df.iterrows():
        # Calculate overall trust (contact info, location data)
        trust_result = calculate_trust_score(row)
        trust_tier = trust_result['trust_tier']
        trust_score = trust_result['trust_score']
        
        # Get capability signals based on specialty documentation
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid")
        )
        trust_str = " | ".join(f"{c}: {TRUST_LABELS[v]}" for c, v in sigs.items())
        
        # Build strengths/weaknesses summary
        strengths = ", ".join(trust_result['strengths'][:3]) if trust_result['strengths'] else "None"
        weaknesses = ", ".join(trust_result['weaknesses'][:3]) if trust_result['weaknesses'] else "None"
        
        desc = (row.get("description") or "")[:300]
        parts.append(
            f"Facility: {row['name']}\n"
            f"Location: {row.get('address_city','')}, {row.get('address_stateorregion','')}\n"
            f"Type: {row.get('facilitytypeid','unknown')}\n"
            f"Overall Trust Score: {trust_score}/100 ({trust_tier.upper()})\n"
            f"  Strengths: {strengths}\n"
            f"  Weaknesses: {weaknesses}\n"
            f"Capability Evidence (from specialty documentation): {trust_str}\n"
            f"Description: {desc}"
        )
    return "\n\n---\n\n".join(parts)

def ask_llm(messages):
    try:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        
        w = WorkspaceClient()
        
        # Convert dict messages to SDK ChatMessage objects
        sdk_messages = []
        for msg in messages:
            role = ChatMessageRole(msg["role"])
            sdk_messages.append(ChatMessage(role=role, content=msg["content"]))
        
        resp = w.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=sdk_messages,
            max_tokens=800,
            temperature=0.2,
        )
        
        # Handle response
        if hasattr(resp, 'choices') and resp.choices:
            return resp.choices[0].message.content
        elif isinstance(resp, dict):
            return resp['choices'][0]['message']['content']
        else:
            return str(resp)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        st.error(f"LLM Error Details:\n{error_details}")
        return f"⚠️ Model error: {e}"

# ── Trust signal logic ────────────────────────────────────────────────────────

def format_phones(raw):
    """Parse messy phone JSON arrays into a clean comma-separated string."""
    if not raw or not isinstance(raw, str):
        return raw
    numbers = re.findall(r'\+?[0-9]{8,15}', raw)
    if not numbers:
        return raw
    seen = set()
    formatted = []
    for num in numbers:
        if not num.startswith('+'):
            if num.startswith('91') and len(num) >= 12:
                num = '+' + num
            elif len(num) == 10:
                num = '+91' + num
        display = f"+91 {num[3:8]} {num[8:]}" if (num.startswith('+91') and len(num) == 13) else num
        if num not in seen:
            seen.add(num)
            formatted.append(display)
    return ', '.join(formatted) if formatted else None

def parse_specialties(raw):
    if not raw:
        return []
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, ValueError):
        return re.findall(r'"([^"]+)"', raw)

# OLD IMPLEMENTATION - Commented out for reference
# def compute_trust_signals(specialties_raw, description, facility_type):
#     """
#     Returns {capability: trust_level} where trust_level is one of:
#       strong  – specialty confirmed by 2+ independent sources in the specialties array
#       partial – specialty listed once in structured specialty data
#       weak    – mentioned only in free-text description, or single hit from
#                 a non-standard facility type (suspicious provenance)
#       none    – no claim found anywhere
#     """
#     specialties = parse_specialties(specialties_raw)
#     desc = (description or "").lower()
#     is_trusted_type = (facility_type or "").lower() in TRUSTED_FACILITY_TYPES
#
#     signals = {}
#     for cap in CAPABILITIES:
#         kws = SPECIALTY_KEYWORDS[cap]
#         hits = sum(1 for s in specialties if any(kw.lower() in s.lower() for kw in kws))
#         desc_hit = any(kw in desc for kw in DESC_KEYWORDS[cap])
#
#         if hits >= 2:
#             signals[cap] = "strong"
#         elif hits == 1:
#             # A single structured claim from a non-standard facility type is suspicious
#             signals[cap] = "partial" if is_trusted_type else "weak"
#         elif desc_hit:
#             signals[cap] = "weak"
#         else:
#             signals[cap] = "none"
#
#     return signals

def calculate_trust_score(row):
    """
    Calculate trust score (0-100) based on weighted signals:
    
    Contact Completeness (40 points):
      - Phone number present: 10 points
      - Email present: 15 points
      - Website present: 15 points
    
    Location Verification (30 points):
      - Latitude & longitude both present: 20 points
      - Complete address (all fields populated): 10 points
    
    Credibility Indicators (30 points):
      - Specialties documented: 15 points
      - Operator type is government/nonprofit: 10 points
      - Last updated within 90 days: 5 points (not available in current data)
    
    Returns: dict with trust_score (0-100), trust_tier (strong/partial/weak),
             strengths list, and weaknesses list
    """
    from datetime import datetime, timedelta
    
    score = 0
    strengths = []
    weaknesses = []
    
    # Contact Completeness (40 points)
    phone = row.get('phone_numbers')
    email = row.get('email')
    website = row.get('website')
    
    if phone and str(phone).strip() and str(phone).lower() not in ['none', 'null', '']:
        score += 10
        strengths.append("Phone number available")
    else:
        weaknesses.append("Missing phone number (-10 pts)")
    
    if email and str(email).strip() and str(email).lower() not in ['none', 'null', '']:
        score += 15
        strengths.append("Email address available")
    else:
        weaknesses.append("Missing email address (-15 pts)")
    
    if website and str(website).strip() and str(website).lower() not in ['none', 'null', '']:
        score += 15
        strengths.append("Website available")
    else:
        weaknesses.append("Missing website (-15 pts)")
    
    # Location Verification (30 points)
    lat = row.get('latitude')
    lon = row.get('longitude')
    has_gps = row.get('has_gps')
    
    # Check if GPS is available either through lat/lon or has_gps flag
    gps_available = False
    if lat and lon and str(lat).strip() and str(lon).strip():
        try:
            float(lat)
            float(lon)
            gps_available = True
        except (ValueError, TypeError):
            pass
    elif has_gps and str(has_gps).lower() in ['yes', 'true', '1']:
        gps_available = True
    
    if gps_available:
        score += 20
        strengths.append("GPS coordinates verified")
    else:
        weaknesses.append("Missing GPS coordinates (-20 pts)")
    
    # Check address completeness
    address_fields = ['address_line1', 'address_city', 'address_stateorregion', 'address_postalcode']
    complete_address = all(
        row.get(field) and str(row.get(field)).strip() and str(row.get(field)).lower() not in ['none', 'null', '']
        for field in address_fields
    )
    
    if complete_address:
        score += 10
        strengths.append("Complete address information")
    else:
        weaknesses.append("Incomplete address details (-10 pts)")
    
    # Credibility Indicators (30 points)
    specialties = row.get('specialties')
    if specialties and str(specialties).strip() and str(specialties) not in ['[]', 'null', 'None', '']:
        try:
            spec_list = parse_specialties(specialties)
            if spec_list and len(spec_list) > 0:
                score += 15
                strengths.append(f"Specialties documented ({len(spec_list)} listed)")
            else:
                weaknesses.append("No specialties documented (-15 pts)")
        except:
            weaknesses.append("No specialties documented (-15 pts)")
    else:
        weaknesses.append("No specialties documented (-15 pts)")
    
    # Operator type check - check both operatortype and operatorTypeId fields
    operator = str(row.get('operatortype', '')).lower()
    operator_id = str(row.get('operatortypeid', '')).lower()
    
    if 'government' in operator or 'nonprofit' in operator or 'public' in operator or \
       'government' in operator_id or 'nonprofit' in operator_id or 'public' in operator_id:
        score += 10
        op_display = row.get('operatortype') or row.get('operatortypeid', 'N/A')
        strengths.append(f"Trusted operator type ({op_display})")
    else:
        weaknesses.append("Not a government/nonprofit operator (-10 pts)")
    
    # Last updated check (5 points) - not available in current dataset
    # Commenting out for now
    # updated = row.get('last_updated')
    # if updated:
    #     try:
    #         updated_date = datetime.fromisoformat(str(updated))
    #         if datetime.now() - updated_date <= timedelta(days=90):
    #             score += 5
    #             strengths.append("Recently updated (within 90 days)")
    #         else:
    #             weaknesses.append("Not updated recently (-5 pts)")
    #     except:
    #         weaknesses.append("Update date unavailable (-5 pts)")
    # else:
    #     weaknesses.append("Update date unavailable (-5 pts)")
    
    # Determine trust tier
    if score >= 70:
        trust_tier = "strong"
    elif score >= 40:
        trust_tier = "partial"
    else:
        trust_tier = "weak"
    
    return {
        'trust_score': score,
        'trust_tier': trust_tier,
        'strengths': strengths,
        'weaknesses': weaknesses
    }

def compute_trust_signals(specialties_raw, description, facility_type):
    """
    Returns {capability: trust_level} for each capability based on specialty documentation.
    
    This measures the STRENGTH OF EVIDENCE for each capability claim:
    - Strong: 2+ independent specialty mentions in structured data (high confidence)
    - Partial: 1 specialty mention from trusted facility type (moderate confidence)
    - Weak: Only in description OR 1 mention from non-trusted type (low confidence)
    - None: Not documented anywhere
    
    Note: This is separate from overall facility trust (contact/location data).
    Both factors should be considered when assessing trustworthiness.
    """
    specialties = parse_specialties(specialties_raw)
    desc = (description or "").lower()
    is_trusted_type = (facility_type or "").lower() in TRUSTED_FACILITY_TYPES

    signals = {}
    for cap in CAPABILITIES:
        kws = SPECIALTY_KEYWORDS[cap]
        hits = sum(1 for s in specialties if any(kw.lower() in s.lower() for kw in kws))
        desc_hit = any(kw in desc for kw in DESC_KEYWORDS[cap])

        # Determine signal from keyword matching - specialty data is authoritative
        if hits >= 2:
            # Multiple specialty mentions = strong evidence regardless of contact info
            signals[cap] = "strong"
        elif hits == 1:
            # Single specialty mention - trust depends on facility type
            signals[cap] = "partial" if is_trusted_type else "weak"
        elif desc_hit:
            # Only in description = weak evidence
            signals[cap] = "weak"
        else:
            # Not documented
            signals[cap] = "none"

    return signals

# ── Rendering helpers ─────────────────────────────────────────────────────────

def _badge(level):
    color = TRUST_COLORS[level]
    label = TRUST_LABELS[level]
    tc = TRUST_TEXT_COLORS[level]
    if level == "none":
        return '<span class="muted-text" style="font-size:13px">—</span>'
    return (
        f'<span style="background:{color};color:{tc};padding:3px 10px;'
        f'border-radius:12px;font-size:11px;font-weight:600;white-space:nowrap">'
        f'{label}</span>'
    )

def render_trust_table(df, caps):
    rows = ""
    for _, row in df.iterrows():
        # Calculate overall trust score
        trust_result = calculate_trust_score(row)
        trust_score = trust_result['trust_score']
        trust_tier = trust_result['trust_tier']
        
        # Get capability signals based on specialty documentation
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid")
        )
        
        city = row.get("address_city") or ""
        state = row.get("address_stateorregion") or ""
        location = f"{city}, {state}".strip(", ")
        ftype = (row.get("facilitytypeid") or "").lower()
        
        # Trust score badge - fixed formatting
        score_color = TRUST_COLORS[trust_tier]
        score_text_color = TRUST_TEXT_COLORS[trust_tier]
        tier_label = TRUST_LABELS[trust_tier].upper()
        score_badge = (
            f'<div style="background:{score_color};color:{score_text_color};padding:6px 12px;'
            f'border-radius:8px;font-size:13px;font-weight:700;display:inline-block;text-align:center;min-width:80px">'
            f'<div>{trust_score}/100</div>'
            f'<div style="font-size:9px;margin-top:2px">{tier_label}</div></div>'
        )
        
        # Consolidate capabilities into a single column as a list with better spacing
        cap_list = []
        for c in caps:
            level = sigs[c]
            if level != "none":
                badge = _badge(level)
                cap_list.append(
                    f'<div style="margin:6px 0;display:flex;align-items:center;line-height:1.6">'
                    f'<strong style="min-width:90px;display:inline-block">{c}:</strong>'
                    f'<span style="margin-left:8px">{badge}</span>'
                    f'</div>'
                )
        
        capabilities_cell = "".join(cap_list) if cap_list else '<span class="muted-text">No capabilities claimed</span>'
        
        # Build strengths/weaknesses tooltip
        strengths_html = " | ".join(f"✓ {s}" for s in trust_result['strengths'])
        weaknesses_html = " | ".join(f"✗ {w}" for w in trust_result['weaknesses'])
        tooltip = f"Strengths: {strengths_html} | Weaknesses: {weaknesses_html}"
        
        rows += (
            f"<tr>"
            f"<td><strong>{row['name']}</strong></td>"
            f'<td class="muted-text" style="font-size:12px">{location}</td>'
            f'<td class="muted-text" style="font-size:12px;text-transform:capitalize">{ftype}</td>'
            f'<td style="text-align:center;padding:10px" title="{tooltip}">{score_badge}</td>'
            f'<td style="font-size:12px">{capabilities_cell}</td>'
            f"</tr>"
        )

    return f"""
<style>
  .tt {{ width:100%;border-collapse:collapse;font-size:13px }}
  .tt th {{ background:#212529;color:white;padding:10px 12px;white-space:nowrap }}
  .tt td {{ padding:10px;border-bottom:1px solid var(--border-soft);vertical-align:top }}
  .tt tr:hover td {{ background:var(--hover-bg) }}
</style>
<table class="tt">
  <thead><tr>
    <th style="text-align:left;width:25%">Facility</th>
    <th style="text-align:left;width:20%">Location</th>
    <th style="text-align:left;width:10%">Type</th>
    <th style="text-align:center;width:12%">Trust Score</th>
    <th style="text-align:left;width:33%">Capabilities</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>"""

def render_facility_details(row):
    """Render detailed trust analysis for a single facility"""
    trust_result = calculate_trust_score(row)
    
    score_color = TRUST_COLORS[trust_result['trust_tier']]
    score_text_color = TRUST_TEXT_COLORS[trust_result['trust_tier']]
    
    strengths_list = "".join(f"<li style='color:#198754'>✓ {s}</li>" for s in trust_result['strengths'])
    weaknesses_list = "".join(f"<li style='color:#dc3545'>✗ {w}</li>" for w in trust_result['weaknesses'])
    
    return f"""
    <div class="panel-box">
        <h4 style="margin-top:0">{row['name']}</h4>
        <div style="margin-bottom:12px">
            <span style="background:{score_color};color:{score_text_color};padding:6px 12px;border-radius:12px;font-size:14px;font-weight:700">
                Trust Score: {trust_result['trust_score']}/100 ({trust_result['trust_tier'].upper()})
            </span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
            <div>
                <h5 style="color:#198754;margin-bottom:8px">✓ Strengths</h5>
                <ul style="margin:0;padding-left:20px">{strengths_list if strengths_list else '<li>None identified</li>'}</ul>
            </div>
            <div>
                <h5 style="color:#dc3545;margin-bottom:8px">✗ Weaknesses</h5>
                <ul style="margin:0;padding-left:20px">{weaknesses_list if weaknesses_list else '<li>None identified</li>'}</ul>
            </div>
        </div>
    </div>
    """

# ── Custom CSS for Genie-style layout ─────────────────────────────────────────

st.markdown("""
<style>
    /* Theme-adaptive colors — light defaults, overridden under dark mode below */
    :root {
        --muted-text: #6c757d;
        --border-soft: #dee2e6;
        --hover-bg: #f8f9fa;
        --panel-bg: rgba(0, 0, 0, 0.02);
        --info-blue-bg: #e3f2fd;
        --info-blue-text: #1976d2;
        --info-orange-bg: #fff3e0;
        --info-orange-text: #f57c00;
        --note-text: #666666;
    }
    @media (prefers-color-scheme: dark) {
        :root {
            --muted-text: #adb5bd;
            --border-soft: #495057;
            --hover-bg: rgba(255, 255, 255, 0.06);
            --panel-bg: rgba(255, 255, 255, 0.04);
            --info-blue-bg: rgba(33, 150, 243, 0.18);
            --info-blue-text: #90caf9;
            --info-orange-bg: rgba(255, 152, 0, 0.18);
            --info-orange-text: #ffcc80;
            --note-text: #adb5bd;
        }
    }

    /* Genie-style chat panel - only target the main 70/30 split, not all columns */
    .stHorizontalBlock > div[data-testid="column"]:nth-child(2) {
        border-left: 3px solid #FF3621;
        padding-left: 20px;
        background: var(--panel-bg);
        display: flex;
        flex-direction: column;
        height: calc(100vh - 200px);
    }

    /* Chat header styling - explicit bg + text so it's readable in both themes */
    .genie-header {
        background: linear-gradient(135deg, #FF3621 0%, #ff6b5a 100%);
        color: white;
        padding: 12px 16px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 16px;
        margin-bottom: 12px;
        flex-shrink: 0;
    }

    /* Chat messages container - grows to fill space */
    .chat-messages-container {
        flex-grow: 1;
        overflow-y: auto;
        display: flex;
        flex-direction: column-reverse;
        padding-right: 8px;
        margin-bottom: 12px;
    }

    /* Chat messages */
    .stChatMessage {
        border-radius: 8px;
        margin-bottom: 12px;
    }

    /* Input area - stays at bottom */
    .chat-input-area {
        flex-shrink: 0;
        padding-top: 8px;
        border-top: 1px solid var(--border-soft);
    }

    /* Disable spinner overlay on chat column only */
    .stHorizontalBlock > div[data-testid="column"]:nth-child(2) .stSpinner {
        position: relative !important;
    }

    .muted-text { color: var(--muted-text); }
    .footer-text { text-align: center; color: var(--note-text); }

    .info-box {
        padding: 15px;
        border-radius: 8px;
        height: 100%;
    }
    .info-box-blue { background: var(--info-blue-bg); border-left: 4px solid #2196f3; }
    .info-box-blue h4 { color: var(--info-blue-text); margin-top: 0; }
    .info-box-orange { background: var(--info-orange-bg); border-left: 4px solid #ff9800; }
    .info-box-orange h4 { color: var(--info-orange-text); margin-top: 0; }
    .info-box .note { margin-top: 12px; margin-bottom: 0; font-size: 12px; color: var(--note-text); }

    .panel-box {
        border: 1px solid var(--border-soft);
        border-radius: 8px;
        padding: 16px;
        margin: 10px 0;
        background: var(--panel-bg);
    }

    .tt td.muted { color: var(--muted-text); }

    /* Header toggle buttons — always visible regardless of theme */
    [data-testid="stHorizontalBlock"]:has([data-testid="column"]) [data-testid="stButton"].toggle-btn > button {
        border: 1.5px solid #555 !important;
        border-radius: 6px !important;
        font-size: 13px !important;
        padding: 4px 8px !important;
    }
</style>
""", unsafe_allow_html=True)

# ── Theme toggle ──────────────────────────────────────────────────────────────

if "app_theme" not in st.session_state:
    st.session_state.app_theme = "dark"

_THEME_CSS = {
    "dark": """
        :root {
            --muted-text: #adb5bd;
            --border-soft: #3d4452;
            --hover-bg: rgba(255,255,255,0.07);
            --panel-bg: rgba(255,255,255,0.05);
            --info-blue-bg: rgba(33,150,243,0.18);
            --info-blue-text: #90caf9;
            --info-orange-bg: rgba(255,152,0,0.18);
            --info-orange-text: #ffcc80;
            --note-text: #adb5bd;
        }
        /* Page background */
        .stApp, [data-testid="stAppViewContainer"],
        [data-testid="stHeader"], [data-testid="block-container"] {
            background-color: #0e1117 !important;
        }
        /* Main text */
        .stApp, .stApp p, .stApp li, .stApp span,
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li { color: #e8eaf0 !important; }
        h1, h2, h3, h4, h5 { color: #f4f6fb !important; }
        /* Metric cards */
        [data-testid="metric-container"] {
            background-color: #1a1d27 !important;
            border: 1px solid #3d4452 !important;
            border-radius: 10px !important;
            padding: 14px !important;
        }
        [data-testid="stMetricValue"] { color: #f4f6fb !important; }
        [data-testid="stMetricLabel"] { color: #adb5bd !important; }
        /* Text inputs */
        [data-baseweb="input"] input,
        [data-baseweb="textarea"] textarea {
            background-color: #1a1d27 !important;
            color: #e8eaf0 !important;
            border-color: #3d4452 !important;
        }
        /* Selectbox */
        [data-baseweb="select"] > div,
        [data-baseweb="select"] [data-testid="stSelectbox"] {
            background-color: #1a1d27 !important;
            color: #e8eaf0 !important;
            border-color: #3d4452 !important;
        }
        /* Dropdown options */
        [data-baseweb="popover"] [role="listbox"],
        [data-baseweb="menu"] { background-color: #1a1d27 !important; }
        [role="option"] { color: #e8eaf0 !important; background-color: #1a1d27 !important; }
        [role="option"]:hover, [aria-selected="true"][role="option"] {
            background-color: #2d3244 !important;
        }
        /* Tabs */
        [data-baseweb="tab-list"] { background-color: #161922 !important; border-bottom: 1px solid #3d4452 !important; }
        [data-baseweb="tab"] { color: #adb5bd !important; }
        [aria-selected="true"][data-baseweb="tab"] { color: #f4f6fb !important; border-bottom-color: #FF3621 !important; }
        /* Expanders */
        [data-testid="stExpander"] details {
            border: 1px solid #3d4452 !important;
            background-color: #161922 !important;
        }
        [data-testid="stExpander"] summary { color: #e8eaf0 !important; }
        /* Dataframe */
        [data-testid="stDataFrame"] { border: 1px solid #3d4452 !important; border-radius: 8px; }
        /* Buttons (secondary/default) */
        .stButton > button:not([kind="primary"]) {
            background-color: #1a1d27 !important;
            border-color: #3d4452 !important;
            color: #e8eaf0 !important;
        }
        .stButton > button:not([kind="primary"]):hover {
            border-color: #FF3621 !important;
            color: #f4f6fb !important;
        }
        /* Caption / small text */
        [data-testid="stCaptionContainer"] p { color: #8b95a7 !important; }
        /* Success / warning / info / error boxes */
        [data-testid="stAlert"] { border-radius: 8px !important; }
        /* Multiselect tags */
        [data-baseweb="tag"] { background-color: #2d3244 !important; color: #e8eaf0 !important; }
        /* Spinner */
        [data-testid="stSpinner"] { color: #adb5bd !important; }
        /* Horizontal rule */
        hr { border-color: #3d4452 !important; }
    """,
    "light": """
        :root {
            --muted-text: #6c757d;
            --border-soft: #dee2e6;
            --hover-bg: #f8f9fa;
            --panel-bg: rgba(0,0,0,0.02);
            --info-blue-bg: #e3f2fd;
            --info-blue-text: #1976d2;
            --info-orange-bg: #fff3e0;
            --info-orange-text: #f57c00;
            --note-text: #555555;
        }
        .stApp, [data-testid="stAppViewContainer"],
        [data-testid="stHeader"], [data-testid="block-container"] {
            background-color: #f5f7fa !important;
        }
        .stApp, .stApp p, .stApp li, .stApp span,
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li { color: #1a1d27 !important; }
        h1, h2, h3, h4, h5 { color: #0e1117 !important; }
        [data-testid="metric-container"] {
            background-color: #ffffff !important;
            border: 1px solid #dee2e6 !important;
            border-radius: 10px !important;
            padding: 14px !important;
        }
        [data-testid="stMetricValue"] { color: #0e1117 !important; }
        [data-testid="stMetricLabel"] { color: #6c757d !important; }
        [data-baseweb="input"] input,
        [data-baseweb="textarea"] textarea {
            background-color: #ffffff !important;
            color: #1a1d27 !important;
            border-color: #dee2e6 !important;
        }
        [data-baseweb="select"] > div { background-color: #ffffff !important; color: #1a1d27 !important; }
        [data-baseweb="tab-list"] { background-color: #ffffff !important; border-bottom: 1px solid #dee2e6 !important; }
        [data-baseweb="tab"] { color: #6c757d !important; }
        [aria-selected="true"][data-baseweb="tab"] { color: #0e1117 !important; border-bottom-color: #FF3621 !important; }
        [data-testid="stExpander"] details { border: 1px solid #dee2e6 !important; background-color: #ffffff !important; }
        .stButton > button:not([kind="primary"]) {
            background-color: #ffffff !important;
            border-color: #dee2e6 !important;
            color: #1a1d27 !important;
        }
        [data-baseweb="tag"] { background-color: #e9ecef !important; color: #1a1d27 !important; }
        hr { border-color: #dee2e6 !important; }
    """,
}
st.markdown(
    f"<style>{_THEME_CSS[st.session_state.app_theme]}</style>",
    unsafe_allow_html=True,
)

# ── Page header ───────────────────────────────────────────────────────────────

is_dark = st.session_state.app_theme == "dark"
if "chat_open" not in st.session_state:
    st.session_state.chat_open = False

title_col, toggle_col = st.columns([6, 6])
with title_col:
    st.title("🏥 Healthcare Facility Capabilities Verification")
with toggle_col:
    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
    t1, t2, t3, t4 = st.columns([1, 1, 1, 3])
    with t1:
        light_type = "primary" if not is_dark else "secondary"
        if st.button("☀️ Light", key="btn_light", type=light_type, use_container_width=True):
            st.session_state.app_theme = "light"
            st.rerun()
    with t2:
        dark_type = "primary" if is_dark else "secondary"
        if st.button("🌙 Dark", key="btn_dark", type=dark_type, use_container_width=True):
            st.session_state.app_theme = "dark"
            st.rerun()
    with t3:
        chat_label = "✕ Chat" if st.session_state.chat_open else "💬 Chat"
        if st.button(chat_label, key="btn_chat_toggle", use_container_width=True):
            st.session_state.chat_open = not st.session_state.chat_open
            st.rerun()

st.markdown("### Data-AVengers Team | DAIS 2026 Virtue Foundation Dataset")

stats = get_stats()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Facilities", f"{stats['total']:,}")
c2.metric("States Covered", stats["states"])
c3.metric("Cities", f"{stats['cities']:,}")
c4.metric("With GPS Coordinates", f"{stats['with_gps']:,}")

st.markdown("---")

# ── Layout: full-width or split depending on chat state ───────────────────────

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if st.session_state.chat_open:
    main_col, chat_col = st.columns([7, 3])
else:
    main_col = st.container()
    chat_col = None

with main_col:
    st.subheader("🔍 Find & Verify a Healthcare Facility")
    st.caption("Search by name, state or city — we'll show contact details, trust scores, and capability evidence all in one place.")

    # ── Unified Search + Trust Evaluation ──────────────────────────────────────

    # Row 1: Facility name (autocomplete) | State | City
    sc1, sc2, sc3 = st.columns([3, 2, 2])
    with sc1:
        facility_names = get_facility_names()
        search_name = st.selectbox(
            "Facility Name",
            [""] + facility_names,
            format_func=lambda x: "— start typing to search —" if x == "" else x,
            key="search_name",
        )
    with sc2:
        states = ["All States"] + get_states()
        selected_state = st.selectbox("State", states, key="search_state")
        state_filter = None if selected_state == "All States" else selected_state
    with sc3:
        city = st.text_input("City", placeholder="e.g., Mumbai, Delhi", key="search_city")

    # Row 2: Trust score slider + capabilities multiselect
    sf1, sf2 = st.columns([2, 3])
    with sf1:
        min_trust_score = st.slider(
            "Minimum Trust Score", 0, 100, 0, step=5,
            help="0 = show all  ·  70+ = high-confidence facilities"
        )
    with sf2:
        selected_caps = st.multiselect(
            "Capabilities to verify",
            CAPABILITIES,
            default=CAPABILITIES,
            key="search_caps",
        )

    search_clicked = st.button("🔎 Search & Verify", type="primary", key="search_btn")

    if search_clicked:
        name_q = search_name if search_name else ""
        with st.spinner("Searching and evaluating facilities…"):
            df = search_and_evaluate(name=name_q, state=state_filter or "", city=city, limit=200)

        if df.empty:
            st.warning("No facilities found. Try broadening your search.")
        else:
            trust_results = [calculate_trust_score(r) for _, r in df.iterrows()]
            df = df.copy()
            df["_trust_score"] = [t["trust_score"] for t in trust_results]
            df["_trust_tier"] = [t["trust_tier"] for t in trust_results]
            df = df[df["_trust_score"] >= min_trust_score].reset_index(drop=True)

            if df.empty:
                st.warning(f"No facilities meet trust score ≥ {min_trust_score}. Try lowering the threshold.")
            else:
                st.success(f"**{len(df)} facilities** found")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Facilities", len(df))
                m2.metric("Avg Trust Score", f"{df['_trust_score'].mean():.0f}/100")
                m3.metric("High Confidence (≥70)", int((df["_trust_score"] >= 70).sum()))
                m4.metric("States", df["address_stateorregion"].nunique())

                st.markdown("---")

                if selected_caps:
                    st.markdown(render_trust_table(df, selected_caps), unsafe_allow_html=True)
                    st.markdown("---")

                # Interactive map with rich tooltip
                map_df = df[["latitude", "longitude", "name", "phone_numbers", "websites", "address_city"]].copy()
                map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
                map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
                map_df = map_df.dropna(subset=["latitude", "longitude"])
                if not map_df.empty:
                    map_df["phone_display"] = map_df["phone_numbers"].apply(format_phones)
                    map_df["website_display"] = map_df["websites"].fillna("").apply(
                        lambda w: w.split(",")[0].strip() if w else "N/A"
                    )
                    map_df["gmaps_query"] = (
                        map_df["name"].fillna("") + " " + map_df["address_city"].fillna("")
                    ).str.replace(" ", "+", regex=False)
                    map_df["gmaps_link"] = (
                        "https://www.google.com/maps/search/?api=1&query=" + map_df["gmaps_query"]
                    )
                    st.subheader(f"📍 Map View ({len(map_df)} facilities with GPS)")
                    layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=map_df,
                        get_position=["longitude", "latitude"],
                        get_radius=8000,
                        get_fill_color=[255, 68, 68, 200],
                        get_line_color=[180, 0, 0],
                        pickable=True,
                    )
                    view = pdk.ViewState(
                        latitude=map_df["latitude"].mean(),
                        longitude=map_df["longitude"].mean(),
                        zoom=5,
                        pitch=0,
                    )
                    st.pydeck_chart(
                        pdk.Deck(
                            layers=[layer],
                            initial_view_state=view,
                            tooltip={
                                "html": (
                                    "<b>{name}</b><br/>"
                                    "📞 {phone_display}<br/>"
                                    "🌐 {website_display}<br/>"
                                    "<a href='{gmaps_link}' target='_blank' "
                                    "style='color:#4fc3f7'>📍 Open in Google Maps</a>"
                                ),
                                "style": {
                                    "backgroundColor": "#1e2736",
                                    "color": "white",
                                    "padding": "10px",
                                    "borderRadius": "6px",
                                },
                            },
                            map_style=(
                                "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
                                if st.session_state.get("app_theme", "dark") == "dark"
                                else "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
                            ),
                        ),
                        use_container_width=True,
                    )
                    st.markdown("---")

                # Expandable detail panels (top 10)
                st.subheader("📊 Detailed Trust Analysis")
                st.caption("Expand a facility to see contact details, trust strengths and weaknesses")
                for _, row in df.head(10).iterrows():
                    score_lbl = f"Trust {row['_trust_score']}/100 · {row['_trust_tier']}"
                    with st.expander(f"🏥 {row['name']} — {row.get('address_city', 'N/A')} ({score_lbl})"):
                        st.markdown(render_facility_details(row), unsafe_allow_html=True)

                # CSV download
                download_rows = []
                for _, row in df.iterrows():
                    trust_result = calculate_trust_score(row)
                    sigs = compute_trust_signals(
                        row.get("specialties"), row.get("description"), row.get("facilitytypeid")
                    )
                    download_rows.append({
                        "facility": row["name"],
                        "city": row.get("address_city", ""),
                        "state": row.get("address_stateorregion", ""),
                        "facility_type": row.get("facilitytypeid", ""),
                        "trust_score": trust_result["trust_score"],
                        "trust_tier": trust_result["trust_tier"],
                        "strengths": " | ".join(trust_result["strengths"]),
                        "weaknesses": " | ".join(trust_result["weaknesses"]),
                        **{f"{c}_trust": TRUST_LABELS[sigs[c]] for c in CAPABILITIES},
                    })
                csv_data = pd.DataFrame(download_rows).to_csv(index=False)
                st.download_button(
                    "⬇️ Download results with trust scores as CSV",
                    csv_data,
                    "facility_trust_analysis.csv",
                    "text/csv",
                )

# ── Collapsible Chat Panel ─────────────────────────────────────────────────────

if st.session_state.chat_open and chat_col is not None:
    with chat_col:
        st.markdown('<div class="genie-header">💬 Facility Trust Advisor</div>', unsafe_allow_html=True)

        if "is_processing" not in st.session_state:
            st.session_state.is_processing = False

        if st.session_state.is_processing:
            st.info("🔄 Retrieving response from AI assistant...")
        else:
            st.caption("Ask about facilities, capabilities, or trust scores")

        st.markdown('<div class="chat-messages-container">', unsafe_allow_html=True)
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="chat-input-area">', unsafe_allow_html=True)

        if not st.session_state.is_processing:
            if prompt := st.chat_input("Ask about facilities…", key="genie_chat_input"):
                st.session_state.is_processing = True
                st.session_state.chat_history.append({"role": "user", "content": prompt})

                terms = extract_search_terms(prompt)
                context_block = None
                if terms and (terms.get("name") or terms.get("state")):
                    df_ctx = search_for_chat(terms)
                    if not df_ctx.empty:
                        context_block = build_chat_context(df_ctx)

                system_content = SYSTEM_PROMPT
                if context_block:
                    system_content += f"\n\n### Relevant facilities from the database:\n\n{context_block}"

                llm_messages = [{"role": "system", "content": system_content}]
                llm_messages += [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.chat_history
                ]

                reply = ask_llm(llm_messages)
                st.session_state.chat_history.append({"role": "assistant", "content": reply})
                st.session_state.is_processing = False
                st.rerun()
        else:
            st.chat_input("Please wait for response...", key="disabled_chat_input", disabled=True)

        st.markdown('</div>', unsafe_allow_html=True)

        if st.session_state.chat_history:
            if st.button("🗑️ Clear", key="clear_genie_chat", use_container_width=True):
                st.session_state.chat_history = []
                st.rerun()

        with st.expander("💡 Examples"):
            st.markdown("""
            - *Can Apollo Hospital do ICU care?*
            - *Facilities in Maharashtra with emergency?*
            - *What is Partial evidence?*
            """)

# ── Footer ────────────────────────────────────────────────────────────────────

with st.expander("📊 About This Project"):
    st.markdown("""
    **Healthcare Facility Capabilities Verification** is built on a medallion architecture:
    * **Bronze Layer**: Raw data ingestion (176,421 rows)
    * **Silver Layer**: Data cleaning & standardization
    * **Gold Layer**: Search-optimized view in Lakebase Postgres

    **Tech Stack:** Databricks Unity Catalog · Lakebase (Postgres) · Streamlit · Python + SQL
    """)

st.markdown("---")
st.markdown(
    "<div class='footer-text'>"
    "<strong>Healthcare Facility Capabilities Verification v2.0</strong><br>"
    "Built with Databricks · Lakebase · Streamlit<br>"
    "Team: Data-AVengers | DAIS 2026 Hackathon"
    "</div>",
    unsafe_allow_html=True,
)
