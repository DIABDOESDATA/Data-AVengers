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
SPECIALTY_KEYWORDS = {
    "ICU":       ["criticalCareMedicine", "criticalCare", "intensiveCare"],
    "Emergency": ["emergencyMedicine", "pediatricEmergencyMedicine",
                  "emergencyPreparedness", "urgentCare"],
    "Maternity": ["gynecologyAndObstetrics", "maternalFetalMedicine",
                  "maternalFetal", "obstetrics",
                  "familyPlanningAndComplexContraception"],
    "Oncology":  ["medicalOncology", "surgicalOncology", "gynecologicalOncology",
                  "gynecologicOncology", "orthopedicOncology",
                  "radiationOncology", "oncology"],
    "Trauma":    ["burnAndTraumaPlasticSurgery", "traumaSurgery", "trauma"],
    "NICU":      ["neonatologyPerinatalMedicine", "neonatology",
                  "neonatalPerinatalMedicine"],
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
        query = """
            SELECT unique_id, name, address_city, address_stateorregion,
                   facilitytypeid, specialties, description
            FROM facilities_search WHERE 1=1
        """
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
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid")
        )
        trust_str = " | ".join(f"{c}: {TRUST_LABELS[v]}" for c, v in sigs.items())
        desc = (row.get("description") or "")[:300]
        parts.append(
            f"Facility: {row['name']}\n"
            f"Location: {row.get('address_city','')}, {row.get('address_stateorregion','')}\n"
            f"Type: {row.get('facilitytypeid','unknown')}\n"
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

def parse_specialties(raw):
    if not raw:
        return []
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, ValueError):
        return re.findall(r'"([^"]+)"', raw)

def compute_trust_signals(specialties_raw, description, facility_type):
    """
    Returns {capability: trust_level} where trust_level is one of:
      strong  – specialty confirmed by 2+ independent sources in the specialties array
      partial – specialty listed once in structured specialty data
      weak    – mentioned only in free-text description, or single hit from
                a non-standard facility type (suspicious provenance)
      none    – no claim found anywhere
    """
    specialties = parse_specialties(specialties_raw)
    desc = (description or "").lower()
    is_trusted_type = (facility_type or "").lower() in TRUSTED_FACILITY_TYPES

    signals = {}
    for cap in CAPABILITIES:
        kws = SPECIALTY_KEYWORDS[cap]
        hits = sum(1 for s in specialties if any(kw.lower() in s.lower() for kw in kws))
        desc_hit = any(kw in desc for kw in DESC_KEYWORDS[cap])

        if hits >= 2:
            signals[cap] = "strong"
        elif hits == 1:
            # A single structured claim from a non-standard facility type is suspicious
            signals[cap] = "partial" if is_trusted_type else "weak"
        elif desc_hit:
            signals[cap] = "weak"
        else:
            signals[cap] = "none"

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
    cap_headers = "".join(f'<th style="text-align:center;min-width:80px">{c}</th>' for c in caps)
    rows = ""
    for _, row in df.iterrows():
        sigs = compute_trust_signals(
            row.get("specialties"), row.get("description"), row.get("facilitytypeid")
        )
        city = row.get("address_city") or ""
        state = row.get("address_stateorregion") or ""
        location = f"{city}, {state}".strip(", ")
        ftype = (row.get("facilitytypeid") or "").lower()
        cells = "".join(f'<td style="text-align:center">{_badge(sigs[c])}</td>' for c in caps)
        rows += (
            f"<tr>"
            f"<td><strong>{row['name']}</strong></td>"
            f'<td style="color:#6c757d;font-size:12px">{location}</td>'
            f'<td style="color:#6c757d;font-size:12px;text-transform:capitalize">{ftype}</td>'
            f"{cells}"
            f"</tr>"
        )

    return f"""
<style>
  .tt {{ width:100%;border-collapse:collapse;font-size:13px }}
  .tt th {{ background:#212529;color:white;padding:8px 12px;white-space:nowrap }}
  .tt td {{ padding:7px 10px;border-bottom:1px solid #dee2e6;vertical-align:middle }}
  .tt tr:hover td {{ background:#f8f9fa }}
</style>
<table class="tt">
  <thead><tr>
    <th style="text-align:left">Facility</th>
    <th style="text-align:left">Location</th>
    <th style="text-align:left">Type</th>
    {cap_headers}
  </tr></thead>
  <tbody>{rows}</tbody>
</table>"""

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
            st.dataframe(results[display_cols], use_container_width=True, hide_index=True)

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
    st.subheader("Evaluate Facility Capability Claims")
    st.markdown(
        "For each facility, this tool evaluates how well its claimed capabilities "
        "are supported by **structured specialty data** vs. unstructured description text."
    )

    with st.expander("📖 Trust signal legend", expanded=True):
        lc = st.columns(4)
        lc[0].markdown(
            f'<span style="background:{TRUST_COLORS["strong"]};color:white;padding:3px 10px;'
            f'border-radius:12px;font-size:12px;font-weight:600">Strong</span>'
            "&nbsp; Specialty confirmed by 2+ independent sources",
            unsafe_allow_html=True,
        )
        lc[1].markdown(
            f'<span style="background:{TRUST_COLORS["partial"]};color:#212529;padding:3px 10px;'
            f'border-radius:12px;font-size:12px;font-weight:600">Partial</span>'
            "&nbsp; Specialty listed once in structured data",
            unsafe_allow_html=True,
        )
        lc[2].markdown(
            f'<span style="background:{TRUST_COLORS["weak"]};color:white;padding:3px 10px;'
            f'border-radius:12px;font-size:12px;font-weight:600">Weak</span>'
            "&nbsp; Description text only, or non-standard facility type",
            unsafe_allow_html=True,
        )
        lc[3].markdown("**—** &nbsp; No claim found in any field", unsafe_allow_html=True)

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
            ["Any claim (Weak+)", "Partial evidence", "Strong evidence"],
            key="min_trust",
        )

    min_level = {"Any claim (Weak+)": 1, "Partial evidence": 2, "Strong evidence": 3}[min_trust_label]

    if st.button("🔬 Evaluate", type="primary", key="eval_btn"):
        if not selected_caps:
            st.warning("Select at least one capability to evaluate.")
        else:
            with st.spinner("Fetching and evaluating facilities…"):
                df_eval = get_facilities_for_evaluation(eval_name, eval_state, limit=200)

            if df_eval.empty:
                st.warning("No facilities found. Try broadening your filters.")
            else:
                # Compute trust signals for every row
                all_signals = [
                    compute_trust_signals(
                        r.get("specialties"), r.get("description"), r.get("facilitytypeid")
                    )
                    for _, r in df_eval.iterrows()
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

                    # CSV download
                    download_rows = []
                    for _, row in df_filtered.iterrows():
                        sigs = compute_trust_signals(
                            row.get("specialties"), row.get("description"), row.get("facilitytypeid")
                        )
                        download_rows.append({
                            "facility": row["name"],
                            "city": row.get("address_city", ""),
                            "state": row.get("address_stateorregion", ""),
                            "facility_type": row.get("facilitytypeid", ""),
                            **{f"{c}_trust": TRUST_LABELS[sigs[c]] for c in CAPABILITIES},
                        })
                    csv = pd.DataFrame(download_rows).to_csv(index=False)
                    st.download_button(
                        "⬇️ Download results as CSV",
                        csv,
                        "capability_trust.csv",
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
