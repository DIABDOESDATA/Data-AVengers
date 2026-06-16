import json
import re
import streamlit as st
import psycopg2
import pandas as pd
from databricks.sdk import WorkspaceClient

st.set_page_config(
    page_title="Healthcare Facility Finder",
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

You help users understand facility capability claims by interpreting structured trust signals derived from specialty data. Trust signal meanings:
- Strong: Specialty confirmed by 2+ independent sources in the structured specialty array (high confidence)
- Partial: Listed once in structured specialty data from a recognized facility type (hospital/clinic/nursing home)
- Weak: Mentioned only in free-text description, OR a single mention from a non-standard facility type (suspicious)
- No Claim (—): Not found anywhere in the data

When facility data is provided below, use it directly to answer. Be concise, direct, and honest. If asked whether a facility can actually do what it claims, give a clear verdict based on the trust signals."""

# ── Database helpers ──────────────────────────────────────────────────────────

@st.cache_resource(ttl=3500)  # Refresh before the 1-hour OAuth token expiry
def get_db_connection():
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
    conn = get_db_connection()
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
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def get_facilities_for_evaluation(search_term="", state="", limit=200):
    conn = get_db_connection()
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
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def get_stats():
    conn = get_db_connection()
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
        return {"total": total, "states": states, "cities": cities, "with_gps": with_gps}
    except Exception as e:
        st.error(f"Stats error: {e}")
        return {"total": 0, "states": 0, "cities": 0, "with_gps": 0}

def get_states():
    conn = get_db_connection()
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
        return states
    except Exception:
        return []

def search_for_chat(query, limit=4):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            """SELECT unique_id, name, address_city, address_stateorregion,
                      facilitytypeid, specialties, description
               FROM facilities_search
               WHERE name ILIKE %s OR description ILIKE %s
               ORDER BY name LIMIT %s""",
            conn,
            params=[f"%{query}%", f"%{query}%", limit],
        )
    except Exception:
        return pd.DataFrame()

def extract_search_terms(text):
    """Pull likely facility name keywords (capitalized sequences) from user message."""
    skip = {"Can", "Which", "What", "Does", "Is", "Are", "Do", "How", "Tell",
            "Show", "Find", "This", "The", "That", "Please", "Give", "List"}
    # Prefer quoted strings
    quoted = re.findall(r'"([^"]+)"', text)
    if quoted:
        return quoted[0]
    # Fall back to capitalized proper-noun runs
    caps = re.findall(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', text)
    meaningful = [c for c in caps if c not in skip]
    return " ".join(meaningful[:4]) if meaningful else ""

def build_chat_context(df):
    """Turn a DataFrame of facilities into a text block for the LLM."""
    if df.empty:
        return None
    parts = []
    for _, row in df.iterrows():
        # Calculate overall trust first
        trust_result = calculate_trust_score(row)
        trust_tier = trust_result['trust_tier']
        trust_score = trust_result['trust_score']
        
        # Get capability signals capped by overall trust
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid"),
            overall_trust_tier=trust_tier
        )
        trust_str = " | ".join(f"{c}: {TRUST_LABELS[v]}" for c, v in sigs.items())
        desc = (row.get("description") or "")[:300]
        parts.append(
            f"Facility: {row['name']}\n"
            f"Location: {row.get('address_city','')}, {row.get('address_stateorregion','')}\n"
            f"Type: {row.get('facilitytypeid','unknown')}\n"
            f"Overall Trust Score: {trust_score}/100 ({trust_tier.upper()})\n"
            f"Capability trust signals: {trust_str}\n"
            f"Description: {desc}"
        )
    return "\n\n---\n\n".join(parts)

def ask_llm(messages):
    try:
        w = WorkspaceClient()
        resp = w.serving_endpoints.query(
            name="databricks-meta-llama-3-3-70b-instruct",
            messages=messages,
            max_tokens=800,
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as e:
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

def compute_trust_signals(specialties_raw, description, facility_type, overall_trust_tier=None):
    """
    Returns {capability: trust_level} for each capability.
    Now adjusted based on overall facility trust score.
    
    If overall trust is weak, capability signals are capped at 'weak' regardless of keyword matches.
    If overall trust is partial, capability signals are capped at 'partial'.
    Only facilities with strong overall trust can show strong capability signals.
    """
    specialties = parse_specialties(specialties_raw)
    desc = (description or "").lower()
    is_trusted_type = (facility_type or "").lower() in TRUSTED_FACILITY_TYPES

    signals = {}
    for cap in CAPABILITIES:
        kws = SPECIALTY_KEYWORDS[cap]
        hits = sum(1 for s in specialties if any(kw.lower() in s.lower() for kw in kws))
        desc_hit = any(kw in desc for kw in DESC_KEYWORDS[cap])

        # Determine base signal from keyword matching
        if hits >= 2:
            base_signal = "strong"
        elif hits == 1:
            base_signal = "partial" if is_trusted_type else "weak"
        elif desc_hit:
            base_signal = "weak"
        else:
            base_signal = "none"
        
        # Cap the signal based on overall facility trust
        if overall_trust_tier:
            if overall_trust_tier == "weak":
                # Weak facilities can't claim strong or partial capabilities
                if base_signal in ["strong", "partial"]:
                    signals[cap] = "weak"
                else:
                    signals[cap] = base_signal
            elif overall_trust_tier == "partial":
                # Partial facilities can't claim strong capabilities
                if base_signal == "strong":
                    signals[cap] = "partial"
                else:
                    signals[cap] = base_signal
            else:
                # Strong facilities can claim any level
                signals[cap] = base_signal
        else:
            signals[cap] = base_signal

    return signals

# ── Rendering helpers ─────────────────────────────────────────────────────────

def _badge(level):
    color = TRUST_COLORS[level]
    label = TRUST_LABELS[level]
    tc = TRUST_TEXT_COLORS[level]
    if level == "none":
        return f'<span style="color:{tc};font-size:13px">—</span>'
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
        
        # Get capability signals (capped by overall trust tier)
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid"),
            overall_trust_tier=trust_tier
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
        
        capabilities_cell = "".join(cap_list) if cap_list else '<span style="color:#999">No capabilities claimed</span>'
        
        # Build strengths/weaknesses tooltip
        strengths_html = " | ".join(f"✓ {s}" for s in trust_result['strengths'])
        weaknesses_html = " | ".join(f"✗ {w}" for w in trust_result['weaknesses'])
        tooltip = f"Strengths: {strengths_html} | Weaknesses: {weaknesses_html}"
        
        rows += (
            f"<tr>"
            f"<td><strong>{row['name']}</strong></td>"
            f'<td style="color:#6c757d;font-size:12px">{location}</td>'
            f'<td style="color:#6c757d;font-size:12px;text-transform:capitalize">{ftype}</td>'
            f'<td style="text-align:center;padding:10px" title="{tooltip}">{score_badge}</td>'
            f'<td style="font-size:12px">{capabilities_cell}</td>'
            f"</tr>"
        )

    return f"""
<style>
  .tt {{ width:100%;border-collapse:collapse;font-size:13px }}
  .tt th {{ background:#212529;color:white;padding:10px 12px;white-space:nowrap }}
  .tt td {{ padding:10px;border-bottom:1px solid #dee2e6;vertical-align:top }}
  .tt tr:hover td {{ background:#f8f9fa }}
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
    <div style="border:1px solid #dee2e6;border-radius:8px;padding:16px;margin:10px 0">
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

# ── Page header ───────────────────────────────────────────────────────────────

st.title("🏥 Healthcare Facility Finder")
st.markdown("### Data-AVengers Team | DAIS 2026 Virtue Foundation Dataset")

stats = get_stats()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Facilities", f"{stats['total']:,}")
c2.metric("States Covered", stats["states"])
c3.metric("Cities", f"{stats['cities']:,}")
c4.metric("With GPS Coordinates", f"{stats['with_gps']:,}")

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["🔍 Facility Search", "🧪 Capability Trust Evaluator", "💬 Chat with Data"])

# ── Tab 1: Facility Search (existing) ────────────────────────────────────────

with tab1:
    st.subheader("Search Healthcare Facilities")

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
            display_cols = [c for c in
                            ["name", "address_city", "address_stateorregion",
                             "phone_numbers", "email", "facilitytypeid"]
                            if c in results.columns]
            display_df = results[display_cols].copy()
            if "phone_numbers" in display_df.columns:
                display_df["phone_numbers"] = display_df["phone_numbers"].apply(format_phones)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            if "latitude" in results.columns and "longitude" in results.columns:
                map_df = results[["latitude", "longitude", "name"]].copy()
                map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
                map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
                map_df = map_df.dropna(subset=["latitude", "longitude"])
                if not map_df.empty:
                    st.subheader(f"📍 Map View ({len(map_df)} facilities with GPS)")
                    st.map(map_df, latitude="latitude", longitude="longitude", size=50, color="#ff4444")
                else:
                    st.info("No GPS coordinates available for the returned facilities.")
        else:
            st.warning("No facilities found matching your search criteria.")

# ── Tab 2: Capability Trust Evaluator ────────────────────────────────────────

with tab2:
    st.subheader("Evaluate Facility Trust & Capability Claims")
    
    # Side-by-side explanation boxes
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background:#e3f2fd;padding:15px;border-radius:8px;border-left:4px solid #2196f3;height:100%">
        <h4 style="margin-top:0;color:#1976d2">📊 Trust Score (0-100)</h4>
        <p style="margin-bottom:12px"><strong>Measures data completeness:</strong></p>
        <div style="font-size:13px;line-height:1.8">
        <p style="margin:6px 0">• Contact info (40 pts): Phone, Email, Website</p>
        <p style="margin:6px 0">• Location (30 pts): GPS + Complete address</p>
        <p style="margin:6px 0">• Credibility (30 pts): Specialties + Operator type</p>
        </div>
        <p style="margin-top:12px;margin-bottom:0;font-size:12px;color:#666">
        <strong>Note:</strong> Reflects database completeness, not medical quality.
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="background:#fff3e0;padding:15px;border-radius:8px;border-left:4px solid #ff9800">
        <h4 style="margin-top:0;color:#f57c00">🏥 Capability Signals</h4>
        <p style="margin-bottom:12px"><strong>6 capabilities evaluated:</strong> ICU, Emergency, Maternity, Oncology, Trauma, NICU</p>
        <div style="font-size:13px;line-height:1.8">
        <p style="margin:6px 0;display:flex;align-items:center">
        <span style="background:{TRUST_COLORS["strong"]};color:white;padding:3px 10px;border-radius:8px;font-size:11px;font-weight:600;display:inline-block;min-width:70px;text-align:center">Strong</span>
        <span style="margin-left:10px">2+ specialty matches</span>
        </p>
        <p style="margin:6px 0;display:flex;align-items:center">
        <span style="background:{TRUST_COLORS["partial"]};color:#212529;padding:3px 10px;border-radius:8px;font-size:11px;font-weight:600;display:inline-block;min-width:70px;text-align:center">Partial</span>
        <span style="margin-left:10px">1 specialty match</span>
        </p>
        <p style="margin:6px 0;display:flex;align-items:center">
        <span style="background:{TRUST_COLORS["weak"]};color:white;padding:3px 10px;border-radius:8px;font-size:11px;font-weight:600;display:inline-block;min-width:70px;text-align:center">Weak</span>
        <span style="margin-left:10px">Description only</span>
        </p>
        </div>
        <p style="margin-top:12px;margin-bottom:0;font-size:12px;color:#666">
        <strong>Note:</strong> Low trust facilities have capabilities capped at "Weak".
        </p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Filters
    fc1, fc2 = st.columns(2)
    with fc1:
        eval_name = st.text_input("Facility name filter", placeholder="e.g., Apollo, Fortis", key="eval_name")
    with fc2:
        eval_states = ["All States"] + get_states()
        eval_state_sel = st.selectbox("State", eval_states, key="eval_state")
        eval_state = None if eval_state_sel == "All States" else eval_state_sel

    fc3, fc4 = st.columns([3, 2])
    with fc3:
        selected_caps = st.multiselect(
            "Capabilities to evaluate",
            CAPABILITIES,
            default=CAPABILITIES,
            key="eval_caps",
        )
    with fc4:
        min_trust_label = st.selectbox(
            "Show only facilities with at least…",
            ["All facilities (no filter)", "Any claim (Weak+)", "Partial evidence", "Strong evidence"],
            key="min_trust",
        )

    min_level = {
        "All facilities (no filter)": 0,
        "Any claim (Weak+)": 1,
        "Partial evidence": 2,
        "Strong evidence": 3
    }[min_trust_label]

    if st.button("🔬 Evaluate", type="primary", key="eval_btn"):
        if not selected_caps:
            st.warning("Select at least one capability to evaluate.")
        else:
            with st.spinner("Fetching and evaluating facilities…"):
                df_eval = get_facilities_for_evaluation(eval_name, eval_state, limit=200)

            if df_eval.empty:
                st.warning("No facilities found. Try broadening your filters.")
            else:
                # Compute trust scores and signals for every row
                all_trust_results = [
                    calculate_trust_score(r)
                    for _, r in df_eval.iterrows()
                ]
                all_signals = [
                    compute_trust_signals(
                        r.get("specialties"), r.get("description"), r.get("facilitytypeid"),
                        overall_trust_tier=trust_result['trust_tier']
                    )
                    for (_, r), trust_result in zip(df_eval.iterrows(), all_trust_results)
                ]

                # Filter by minimum trust threshold across selected capabilities
                mask = [
                    any(TRUST_ORDER[sig.get(c, "none")] >= min_level for c in selected_caps)
                    for sig in all_signals
                ]
                df_filtered = df_eval[mask].reset_index(drop=True)

                if df_filtered.empty:
                    st.warning("No facilities meet the selected trust threshold. Try lowering it.")
                else:
                    st.success(f"**{len(df_filtered)} facilities** match your filters")

                    # Capability summary metrics
                    mcols = st.columns(len(CAPABILITIES))
                    for i, cap in enumerate(CAPABILITIES):
                        count = sum(
                            1 for sig in all_signals
                            if TRUST_ORDER[sig.get(cap, "none")] >= 2
                        )
                        mcols[i].metric(cap, f"{count:,}", help=f"Facilities with ≥ Partial evidence for {cap}")

                    st.markdown("---")

                    # Trust matrix table
                    st.markdown(
                        render_trust_table(df_filtered, selected_caps),
                        unsafe_allow_html=True,
                    )
                    
                    # Detailed facility analysis
                    st.markdown("---")
                    st.subheader("📊 Detailed Trust Analysis")
                    st.caption("Click on a facility to see strengths and weaknesses")
                    
                    # Show detailed view for top 10 facilities
                    for idx, (_, row) in enumerate(df_filtered.head(10).iterrows()):
                        with st.expander(f"🏥 {row['name']} - {row.get('address_city', 'N/A')}"):
                            st.markdown(render_facility_details(row), unsafe_allow_html=True)

                    # CSV download with trust scores
                    download_rows = []
                    for _, row in df_filtered.iterrows():
                        trust_result = calculate_trust_score(row)
                        sigs = compute_trust_signals(
                            row.get("specialties"), row.get("description"), row.get("facilitytypeid"),
                            overall_trust_tier=trust_result['trust_tier']
                        )
                        download_rows.append({
                            "facility": row["name"],
                            "city": row.get("address_city", ""),
                            "state": row.get("address_stateorregion", ""),
                            "facility_type": row.get("facilitytypeid", ""),
                            "trust_score": trust_result['trust_score'],
                            "trust_tier": trust_result['trust_tier'],
                            "strengths": " | ".join(trust_result['strengths']),
                            "weaknesses": " | ".join(trust_result['weaknesses']),
                            **{f"{c}_trust": TRUST_LABELS[sigs[c]] for c in CAPABILITIES},
                        })
                    csv = pd.DataFrame(download_rows).to_csv(index=False)
                    st.download_button(
                        "⬇️ Download results with trust scores as CSV",
                        csv,
                        "facility_trust_analysis.csv",
                        "text/csv",
                    )

# ── Tab 3: Chat with Data ─────────────────────────────────────────────────────

with tab3:
    st.subheader("💬 Chat with Your Facility Data")
    st.caption(
        "Ask questions like: *Can Apollo Hospital actually do ICU care?* · "
        "*Which facilities in Maharashtra have strong emergency claims?* · "
        "*What does Partial evidence mean for maternity?*"
    )

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Render conversation history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about facilities or their capability claims…"):
        # Show user message immediately
        with st.chat_message("user"):
            st.markdown(prompt)
        st.session_state.chat_history.append({"role": "user", "content": prompt})

        # Search the DB for relevant facilities
        terms = extract_search_terms(prompt)
        context_block = None
        if terms:
            df_ctx = search_for_chat(terms)
            context_block = build_chat_context(df_ctx)

        # Build system message with optional facility context
        system_content = SYSTEM_PROMPT
        if context_block:
            system_content += f"\n\n### Relevant facilities from the database:\n\n{context_block}"

        llm_messages = [{"role": "system", "content": system_content}]
        llm_messages += [
            {"role": m["role"], "content": m["content"]}
            for m in st.session_state.chat_history
        ]

        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                reply = ask_llm(llm_messages)
            st.markdown(reply)

        st.session_state.chat_history.append({"role": "assistant", "content": reply})

        if st.button("🗑️ Clear conversation", key="clear_chat"):
            st.session_state.chat_history = []
            st.rerun()

# ── Footer ────────────────────────────────────────────────────────────────────

with st.expander("📊 About This Project"):
    st.markdown("""
    **Healthcare Facility Finder** is built on a medallion architecture:
    * **Bronze Layer**: Raw data ingestion (176,421 rows)
    * **Silver Layer**: Data cleaning & standardization
    * **Gold Layer**: Search-optimized view in Lakebase Postgres

    **Tech Stack:** Databricks Unity Catalog · Lakebase (Postgres) · Streamlit · Python + SQL
    """)

st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#666'>"
    "<strong>Healthcare Facility Finder v2.0</strong><br>"
    "Built with Databricks · Lakebase · Streamlit<br>"
    "Team: Data-AVengers | DAIS 2026 Hackathon"
    "</div>",
    unsafe_allow_html=True,
)
