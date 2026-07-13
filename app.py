import streamlit as st
import pandas as pd
import plotly.express as px

# ==============================================================================
# 0. PAGE CONFIGURATION (top and vertical navbars + pages) & THEME ALIGNMENT
# ==============================================================================
st.set_page_config(
    page_title="Rebate Intelligence Pipeline Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for dark navy theme alignment (#0A1128) and clean teal accents (#00B4D8)
# 2. Strict Theme Overrides (Dark Navy & Teal Accents)
st.markdown("""
    <style>
        /* Force app canvas color background and default light font text */
        .stApp { 
            background-color: #0A1128; 
            color: #FAFAFA; 
        }
        
        /* Typography overrides */
        h1, h2, h3, h4, h5, h6, p, span, label, div { 
            color: #FAFAFA !important; 
        }
        
        /* Sidebar container styling */
        section[data-testid="stSidebar"] {
            background-color: #0D1B3E !important;
        }
        
        /* Tab interactive styles */
        .stTabs [data-baseweb="tab"] { 
            color: #A0AAB2 !important; 
            font-weight: 600; 
        }
        .stTabs [data-baseweb="tab"]:hover { 
            color: #00B4D8 !important; 
        }
        .stTabs [aria-selected="true"] { 
            color: #00B4D8 !important; 
            border-bottom-color: #00B4D8 !important; 
        }
        
        /* Metric Card specific styling */
        div[data-testid="stMetricValue"] { 
            color: #00B4D8 !important; 
            font-size: 2.2rem !important; 
            font-weight: bold !important; 
        }
        div[data-testid="stMetricLabel"] { 
            color: #A0AAB2 !important; 
        }
    </style>
""", unsafe_allow_html=True)



# ==============================================================================
# 1. INITIALIZE SNOWFLAKE CONNECTION
# ==============================================================================
try:
    conn = st.connection("snowflake")
    connection_status = "🟢 Connection Status\n`Snowflake Live`"
    connection_live = True
except Exception as e:
    connection_status = "🔴 Connection Status\n`Handshake Offline`"
    connection_live = False
    st.error("Credential handshake failed. Check your .streamlit/secrets.toml file.")
    st.exception(e)
    st.stop()

# # 2. Inspect Table Columns Safely
# st.markdown("### 🔍 Investigating Table Schemas...")

# for table in ["DIM_PARTNERS", "FACT_REBATE_PAYOUTS", "MART_AFFILIATE_SUMMARY"]:
#     try:
#         # Fetch just 1 row to see what columns actually exist
#         df = conn.query(f"SELECT * FROM {table} LIMIT 1;") # a streamlit quirk, we have to select a row in order to get details about the table. Limit 1 ofc to reduce processing and data tarnsit.
#         st.write(f"**Table:** `{table}`")
#         st.write("Columns found:", list(df.columns))
#     except Exception as e:
#         st.error(f"Could not read from table `{table}`. Does it exist in DEV_MCP_DB.PUBLIC?")
#         st.exception(e)

# ==============================================================================
# 3. SIDEBAR NAVIGATION & GLOBAL FILTERS
# ==============================================================================
# Sidebar Connection Handshake Indicator
st.sidebar.title("Pipeline Controls")
st.sidebar.markdown("### 🟢 Connection Status\n`Snowflake Live`")

# 4. Navbar & Intrenal Tabs
tab1, tab2, tab3 = st.tabs([
    "📊 Executive Overview", 
    "🕵️‍♂️ Silent Shop Intelligence", 
    "🏗️ Data Lineage & Stack Rationale"
])




# ------------------------------------------------------------------------------
# TAB 1: EXECUTIVE OVERVIEW
# ------------------------------------------------------------------------------
with tab1:
    st.title("Rebate Activity Executive Dashboard")
    st.markdown("### Real-Time Performance & Enterprise Payouts")
    if connection_live:
        try:
            ##########################
            # Headline KPIs
            ##########################

            # 1. Total Rebates Payouts
            payouts_df = conn.query("SELECT SUM(rebate_amount) as total FROM FACT_REBATE_PAYOUTS;")
            total_payouts = payouts_df['TOTAL'].iloc[0] or 0
            
            # 2. Active Affiliates (is_silent_shop = false)
            active_df = conn.query("SELECT COUNT(*) as count FROM MART_AFFILIATE_SUMMARY WHERE is_silent_shop = false;")
            active_count = active_df['COUNT'].iloc[0] or 0
            
            # 3. Silent Shops Identified (Dynamic query for data provenance tracking)
            silent_df = conn.query("SELECT COUNT(*) as count FROM MART_AFFILIATE_SUMMARY WHERE is_silent_shop = true;")
            silent_count = silent_df['COUNT'].iloc[0] or 0
            
            # 4. Pipeline Exception Rates
            exception_df = conn.query("SELECT (SUM(exception_count) / NULLIF(SUM(total_transactions), 0)) * 100 as rate FROM MART_AFFILIATE_SUMMARY;")
            exception_rate = exception_df['RATE'].iloc[0] or 0

            # Render KPI Grid Layout
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Rebate Payouts", f"${total_payouts:,.2f}")
            col2.metric("Active Affiliates", f"{active_count:,}")
            
            # Displaying the resampled dataset count (75) with context
            col3.metric(label="Silent Shops Identified", value=int(silent_count))
            col4.metric("Exception Rate", f"{exception_rate:.2f}%")
            
            ##########################
            ### Visualizations Layer
            ##########################

            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                st.markdown("#### Rebate Activity Over Time")
                # Querying time dimensions built out during your Silver processing layer
                trend_df = conn.query("""
                    SELECT 
                        CAST(transaction_year AS VARCHAR) || '-' || LPAD(CAST(transaction_month AS VARCHAR), 2, '0') as period,
                        SUM(rebate_amount) as monthly_rebate
                    FROM FACT_REBATE_PAYOUTS
                    GROUP BY transaction_year, transaction_month
                    ORDER BY period ASC;
                """)
                
                fig_line = px.line(
                    trend_df, 
                    x='PERIOD', 
                    y='MONTHLY_REBATE', 
                    template="plotly_dark", 
                    labels={'MONTHLY_REBATE': 'Payout Value ($)', 'PERIOD': 'Timeline'}
                )
                fig_line.update_traces(line_color='#00B4D8', line_shape='linear')
                fig_line.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis_title=None
                )
                st.plotly_chart(fig_line, use_container_width=True)
                
            with chart_col2:
                st.markdown("#### Top 10 Partners by Total Rebate Payout")
                # Join Fact with Partner dimensions to pull readable names instead of raw IDs
                partner_df = conn.query("""
                    SELECT p.partner_name, SUM(f.rebate_amount) as total_payout
                    FROM FACT_REBATE_PAYOUTS f
                    JOIN DIM_PARTNERS p ON f.partner_id = p.partner_id
                    GROUP BY p.partner_name 
                    ORDER BY total_payout DESC 
                    LIMIT 10;
                """)
                
                fig_bar = px.bar(
                    partner_df, 
                    x='TOTAL_PAYOUT', 
                    y='PARTNER_NAME', 
                    orientation='h', 
                    template="plotly_dark", 
                    labels={'TOTAL_PAYOUT': 'Total Payout ($)', 'PARTNER_NAME': 'Partner'}
                )
                fig_bar.update_layout(
                    yaxis={'categoryorder':'total ascending'},
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    yaxis_title=None
                )
                fig_bar.update_traces(marker_color='#00B4D8')
                st.plotly_chart(fig_bar, use_container_width=True)
                
            st.markdown("---")
            st.markdown("#### Rebate Distribution by Program Tier")
            # Proving out the tier finding visually
            tier_df = conn.query("""
                SELECT program_tier, COUNT(*) as site_count 
                FROM MART_AFFILIATE_SUMMARY 
                GROUP BY program_tier 
                ORDER BY site_count DESC;
            """)
            
            fig_tier = px.bar(
                tier_df, 
                x='PROGRAM_TIER', 
                y='SITE_COUNT', 
                template="plotly_dark",
                labels={'PROGRAM_TIER': 'Program Tier', 'SITE_COUNT': 'Location Count'}
            )
            fig_tier.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis_title=None
            )
            fig_tier.update_traces(marker_color='#00B4D8')
            st.plotly_chart(fig_tier, use_container_width=True)
            
        except Exception as e:
            st.error("Error pulling database metrics for summary charts.")
            st.exception(e)
            
  

# ------------------------------------------------------------------------------
# TAB 2: SILENT SHOP INTELLIGENCE (ANOMALY DETECTION)
# ------------------------------------------------------------------------------
with tab2:
    st.title("Silent Shop Operational Intelligence")
    st.warning(
        "**Note on Data Distribution:** This dashboard queries a fully synthesized dataset. "
        "Silent shop distribution across partners and states may not reflect authentic data "
        "spread. Findings represent the same detection logic applied to original data."
    )
    
    # 1. Contextual Executive Callout
    st.markdown("#### What is a Silent Shop?")

    st.info(
    "A silent shop is an affiliate location that appears in the network's master "
    "roster (DIM_PARTNER) but has zero valid rebate transactions on record. Every transaction "
    "they submitted was flagged as an anomaly — meaning no legitimate dollar "
    "activity can be confirmed. These locations may represent unrecovered rebates "
    "and are surfaced here for operational follow-up."
)
    
    if connection_live:
        try:
            # 2. Query the exact Anomaly Ledger rows from the Gold layer
            ledger_query = """
                SELECT 
                    affiliate_name AS "Affiliate Name", 
                    state AS "State", 
                    program_tier AS "Program Tier", 
                    partner_name AS "Partner Name", 
                    total_transactions AS "Total Transactions", 
                    exception_count AS "Flagged Transactions"
                FROM MART_AFFILIATE_SUMMARY
                WHERE is_silent_shop = true
                ORDER BY exception_count DESC;
            """
            ledger_df = conn.query(ledger_query)
            
            # Display interactive audit table
            st.markdown("### Silent Shops Ledger")
            st.dataframe(ledger_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # 3. Anomaly Breakdown Charts Split into Two Columns
            geo_col, exc_col = st.columns(2)
            
            with geo_col:
                st.markdown("#### Geographic Distribution of Silent Shops")
                # Aggregate rows to show concentration by state
                geo_df = ledger_df.groupby('State').size().reset_index(name='Count').sort_values('Count', ascending=False)
                
                fig_geo = px.bar(
                    geo_df, 
                    x='State', 
                    y='Count', 
                    template="plotly_dark", 
                    labels={'Count': 'Silent Shop Count'}
                )
                fig_geo.update_traces(marker_color='#00B4D8')
                fig_geo.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis_title=None
                )
                st.plotly_chart(fig_geo, use_container_width=True)
                
            with exc_col:
                st.markdown("#### Flagged Transactions by Program Tier — Silent Shops Only")
                # Aggregate total exception counts caught by our Silver-layer rules
                exc_df = ledger_df.groupby('Program Tier')['Flagged Transactions'].sum().reset_index().sort_values('Flagged Transactions', ascending=False)
                
                fig_exc = px.bar(
                    exc_df, 
                    x='Program Tier', 
                    y='Flagged Transactions', 
                    template="plotly_dark", 
                    labels={'Flagged Transactions': 'Anomalous Transaction Count'}
                )
                fig_exc.update_traces(marker_color='#00B4D8')
                fig_exc.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    xaxis_title=None
                )
                st.plotly_chart(fig_exc, use_container_width=True)
                st.caption("Note: Distribution reflects synthesized dataset. Production data spans a broader partner population.")

        except Exception as e:
            st.error("Error executing dynamic anomaly queries against Snowflake data layers.")
            st.exception(e)

# ------------------------------------------------------------------------------
# TAB 3: DATA LINEAGE & STACK RATIONALE
# # ------------------------------------------------------------------------------
with tab3:
    st.title("How This Was Built")

    st.markdown("""
    This dashboard is the final layer of a two-phase data engineering project. 
    The pipeline was originally built using Python and Microsoft Fabric with a Power BI 
    dashboard for a regional collision shop network. Phase 2 rebuilt the transformation 
    layer on a modern composable stack — Snowflake, dbt, and Streamlit — to demonstrate 
    the patterns used in enterprise data environments.

    ---

    ### Why Snowflake + dbt Instead of Microsoft Fabric?

    **Microsoft Fabric** is the right tool for the small, stable team that only needed a single 
    output — a Power BI dashboard — in a Microsoft-native environment. That was Phase 1.

    **Snowflake + dbt** is very often the better tool when multiple teams consume the same data 
    in different ways, or when transformation logic is complex enough to need version 
    control, in addition to automated testing, and a visible lineage graph. 
    
    Very often, if team growth is expected these needs are inevitable and should be built 
    in place foundationally. 
    
    3 specific reasons drove the choice here:

    - **Transformations live in Git.** Every model is a SQL file with a commit history. 
      Nothing is hidden inside a visual editor or a notebook.
    - **Testing is built in.** dbt runs automated checks on every table before any 
      downstream layer reads from it — uniqueness, nulls, referential integrity.
    - **Lineage is automatic.** dbt generates an interactive dependency graph showing 
      exactly how data flows from raw source to final mart, which you can explore below.
    """)

    st.markdown("---")

    l_col1, l_col2, l_col3 = st.columns(3)

    with l_col1:
        st.metric("dbt Tests", "25 Passing", delta="9 Models Covered")
        st.markdown(
            "Every table in this dashboard is protected by automated tests "
            "checking for uniqueness, null values, and referential integrity "
            "between fact and dimension tables."
        )

    with l_col2:
        st.markdown("#### Interactive Lineage Graph")
        st.markdown(
            "Explore the full data flow from raw source tables through staging, "
            "intermediate, and mart layers — built automatically by dbt."
        )
        st.link_button(
            "🌐 Open Lineage Graph",
            "https://emorain3.github.io/rebate-intelligence-pipeline/"
        )

    with l_col3:
        st.markdown("#### Full Case Study")
        st.markdown(
            "The complete writeup covers the business context, architecture decisions, "
            "anomaly detection design, and commercial impact framing."
        )
        st.link_button(
            "📄 Read the Case Study",
            "https://emorain3.github.io/rebate-intelligence-pipeline-casestudy/"
        )