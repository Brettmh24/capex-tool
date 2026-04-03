"""
CapEx Asset Replacement Tool
Upload an asset spreadsheet → auto-enrich → age bucket dashboard → replacement recommendations → ROI calculator
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
from io import BytesIO
from enrichment import load_and_clean, enrich_dataframe, calculate_roi, get_enrichment_summary

st.set_page_config(page_title="CapEx Asset Replacement Tool", page_icon="🏗️", layout="wide")

# --- Custom CSS ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem; font-weight: 700; color: #1a1a2e;
        padding-bottom: 0.5rem; border-bottom: 3px solid #0066cc;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.2rem; border-radius: 12px; color: white; text-align: center;
    }
    .metric-card h3 { margin: 0; font-size: 0.85rem; opacity: 0.9; }
    .metric-card h1 { margin: 0.3rem 0 0 0; font-size: 2rem; }
    .priority-critical { color: #dc3545; font-weight: 700; }
    .priority-high { color: #fd7e14; font-weight: 700; }
    .priority-medium { color: #ffc107; font-weight: 600; }
    .priority-low { color: #28a745; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px; border-radius: 8px 8px 0 0; font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">CapEx Asset Replacement Tool</div>', unsafe_allow_html=True)


# --- SESSION STATE ---
if "enriched_df" not in st.session_state:
    st.session_state.enriched_df = None
if "raw_df" not in st.session_state:
    st.session_state.raw_df = None


# --- FILE UPLOAD ---
with st.sidebar:
    st.header("📁 Upload Asset Data")
    uploaded_file = st.file_uploader(
        "Drop your asset spreadsheet here",
        type=["xlsx", "xls", "csv"],
        help="Upload an Excel or CSV file with asset data. Must contain columns like Serial No., Brand, Model No., etc."
    )

    if uploaded_file:
        with st.spinner("Loading and enriching your data..."):
            raw_df = load_and_clean(uploaded_file)
            st.session_state.raw_df = raw_df
            enriched_df = enrich_dataframe(raw_df)
            st.session_state.enriched_df = enriched_df

        summary = get_enrichment_summary(enriched_df)
        st.success(f"✅ Loaded {summary['total_assets']} assets")
        st.metric("Date Coverage", f"{summary['date_coverage_pct']}%")
        st.caption(f"Dates found for {summary['assets_with_date']} of {summary['total_assets']} assets")

        st.subheader("Date Sources")
        for source, count in summary.get("source_breakdown", {}).items():
            st.caption(f"• {source}: {count}")

    st.divider()
    st.caption(f"📅 Today: {datetime.now().strftime('%B %d, %Y')}")
    st.caption("Ages calculated in real-time against today's date.")


# --- MAIN CONTENT ---
if st.session_state.enriched_df is None:
    st.info("👈 Upload an asset spreadsheet to get started. The tool will automatically enrich missing data, bucket assets by age, and recommend replacements.")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### 1️⃣ Upload")
        st.write("Drop your Excel/CSV asset list in the sidebar. We'll parse it automatically.")
    with col2:
        st.markdown("### 2️⃣ Enrich")
        st.write("Serial numbers are decoded to fill in missing manufacture dates, capacity, and specs.")
    with col3:
        st.markdown("### 3️⃣ Analyze")
        st.write("Get age buckets, replacement priority scores, and ROI calculations instantly.")

else:
    df = st.session_state.enriched_df
    summary = get_enrichment_summary(df)

    # --- TOP METRICS ---
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric("Total Assets", f"{summary['total_assets']:,}")
    with m2:
        st.metric("Avg Age", f"{summary['avg_age']} yrs" if summary['avg_age'] else "N/A")
    with m3:
        critical = summary.get("priority_breakdown", {}).get("Critical", 0)
        st.metric("Critical", critical)
    with m4:
        high = summary.get("priority_breakdown", {}).get("High", 0)
        st.metric("High Priority", high)
    with m5:
        st.metric("Facilities", summary["facilities_count"])

    # --- TABS ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Age Buckets", "🎯 Replacement Priority", "💰 ROI Calculator", "📋 Full Data", "📥 Export"
    ])

    # ===================== TAB 1: AGE BUCKETS =====================
    with tab1:
        st.subheader("Asset Age Distribution")

        # Filters
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            facility_filter = st.multiselect(
                "Filter by Facility",
                options=sorted(df["facility_name"].dropna().unique()) if "facility_name" in df.columns else [],
                default=[]
            )
        with fcol2:
            brand_filter = st.multiselect(
                "Filter by Brand",
                options=sorted(df["brand"].dropna().unique()) if "brand" in df.columns else [],
                default=[]
            )
        with fcol3:
            type_filter = st.multiselect(
                "Filter by Asset Type",
                options=sorted(df["asset_type"].dropna().unique()) if "asset_type" in df.columns else [],
                default=[]
            )

        filtered = df.copy()
        if facility_filter:
            filtered = filtered[filtered["facility_name"].isin(facility_filter)]
        if brand_filter:
            filtered = filtered[filtered["brand"].isin(brand_filter)]
        if type_filter:
            filtered = filtered[filtered["asset_type"].isin(type_filter)]

        # Build bucket chart
        bucket_df = filtered[filtered["age_bucket"] != "Unknown"].copy()
        if not bucket_df.empty:
            # Create ordered buckets
            bucket_order = ["< 1 Year"] + [f"{i} Years" for i in range(1, 31)] + ["30+ Years"]
            existing_buckets = [b for b in bucket_order if b in bucket_df["age_bucket"].values]

            bucket_counts = bucket_df["age_bucket"].value_counts()
            chart_data = pd.DataFrame({
                "Age Bucket": existing_buckets,
                "Count": [bucket_counts.get(b, 0) for b in existing_buckets]
            })

            # Color gradient: green (new) → yellow → red (old)
            def bucket_color(bucket):
                if bucket == "< 1 Year":
                    return "#28a745"
                elif bucket == "30+ Years":
                    return "#dc3545"
                else:
                    try:
                        yr = int(bucket.split()[0])
                    except ValueError:
                        return "#6c757d"
                    if yr <= 5:
                        return "#28a745"
                    elif yr <= 10:
                        return "#7cb342"
                    elif yr <= 15:
                        return "#ffc107"
                    elif yr <= 20:
                        return "#fd7e14"
                    elif yr <= 25:
                        return "#e74c3c"
                    else:
                        return "#dc3545"

            colors = [bucket_color(b) for b in chart_data["Age Bucket"]]

            fig = go.Figure(go.Bar(
                x=chart_data["Age Bucket"],
                y=chart_data["Count"],
                marker_color=colors,
                text=chart_data["Count"],
                textposition="outside",
            ))
            fig.update_layout(
                title="Assets by Manufacture Year Age",
                xaxis_title="Age Bucket",
                yaxis_title="Number of Assets",
                height=500,
                xaxis_tickangle=-45,
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

            # Summary stats below chart
            sc1, sc2, sc3, sc4 = st.columns(4)
            aged_10plus = len(bucket_df[bucket_df["asset_age_years"] >= 10])
            aged_20plus = len(bucket_df[bucket_df["asset_age_years"] >= 20])
            with sc1:
                st.metric("Assets Shown", len(filtered))
            with sc2:
                st.metric("With Age Data", len(bucket_df))
            with sc3:
                st.metric("10+ Years Old", aged_10plus)
            with sc4:
                st.metric("20+ Years Old", aged_20plus)
        else:
            st.warning("No age data available for the selected filters.")

        # Show unknowns
        unknown_count = len(filtered[filtered["age_bucket"] == "Unknown"])
        if unknown_count > 0:
            st.warning(f"⚠️ {unknown_count} assets have no determinable manufacture date (serial could not be decoded and no install date available).")

    # ===================== TAB 2: REPLACEMENT PRIORITY =====================
    with tab2:
        st.subheader("Replacement Recommendations")

        priority_col1, priority_col2 = st.columns([2, 1])

        with priority_col2:
            st.markdown("### Priority Legend")
            st.markdown("""
            - 🔴 **Critical** (70-100): Past lifespan, poor/broken condition
            - 🟠 **High** (50-69): Approaching end of life
            - 🟡 **Medium** (30-49): Aging, monitor closely
            - 🟢 **Low** (15-29): Healthy, plan ahead
            - ⚪ **No Action** (0-14): Good condition, newer
            """)

            st.markdown("### Scoring Factors")
            st.markdown("""
            - **Age vs Lifespan**: Up to 50 pts
            - **Condition**: Up to 30 pts
            - **Absolute Age**: Up to 20 pts
            """)

        with priority_col1:
            # Priority donut chart
            priority_counts = filtered["replacement_priority"].value_counts()
            priority_colors = {
                "Critical": "#dc3545", "High": "#fd7e14", "Medium": "#ffc107",
                "Low": "#28a745", "No Action": "#6c757d", "Unknown": "#adb5bd"
            }
            fig_donut = go.Figure(go.Pie(
                labels=priority_counts.index,
                values=priority_counts.values,
                hole=0.5,
                marker_colors=[priority_colors.get(p, "#999") for p in priority_counts.index],
                textinfo="label+value",
            ))
            fig_donut.update_layout(title="Replacement Priority Distribution", height=400)
            st.plotly_chart(fig_donut, use_container_width=True)

        # Top replacement candidates table
        st.subheader("Top Replacement Candidates")
        priority_filter = st.multiselect(
            "Filter by Priority",
            options=["Critical", "High", "Medium", "Low", "No Action"],
            default=["Critical", "High"]
        )

        display_cols = ["asset_tag", "brand", "model_no", "facility_name", "asset_type",
                        "condition_clean", "best_mfg_year", "asset_age_years",
                        "expected_lifespan_years", "life_consumed_pct",
                        "replacement_score", "replacement_priority", "capacity_tons"]
        display_cols = [c for c in display_cols if c in filtered.columns]

        priority_df = filtered[filtered["replacement_priority"].isin(priority_filter)][display_cols].sort_values(
            "replacement_score", ascending=False
        )

        st.dataframe(priority_df, use_container_width=True, height=500)

    # ===================== TAB 3: ROI CALCULATOR =====================
    with tab3:
        st.subheader("ROI Calculator — Replace vs. Keep")
        st.write("Select assets to replace and model the financial impact.")

        roi_col1, roi_col2 = st.columns(2)

        with roi_col1:
            st.markdown("### Selection")
            select_method = st.radio(
                "Select assets to replace by:",
                ["Priority Level", "Age Range", "Facility", "Manual Count"],
                horizontal=True
            )

            if select_method == "Priority Level":
                roi_priority = st.multiselect("Priority", ["Critical", "High", "Medium"], default=["Critical"])
                selected = filtered[filtered["replacement_priority"].isin(roi_priority)]
            elif select_method == "Age Range":
                min_age, max_age = st.slider("Age Range (years)", 0, 35, (15, 30))
                selected = filtered[(filtered["asset_age_years"] >= min_age) & (filtered["asset_age_years"] <= max_age)]
            elif select_method == "Facility":
                roi_facility = st.selectbox("Facility", sorted(filtered["facility_name"].dropna().unique()))
                selected = filtered[filtered["facility_name"] == roi_facility]
            else:
                manual_count = st.number_input("Number of units to replace", min_value=1, value=10)
                selected = filtered.head(manual_count)

            st.metric("Units Selected", len(selected))

        with roi_col2:
            st.markdown("### Assumptions")
            replacement_cost = st.number_input(
                "Replacement cost per unit ($)", min_value=0, value=8500, step=500,
                help="Average cost to purchase and install a new unit"
            )
            current_spend = st.number_input(
                "Current annual maintenance spend on selected units ($)",
                min_value=0, value=5000, step=500,
                help="Total annual spend on maintenance/repairs for these units"
            )
            filter_cost = st.number_input(
                "Quarterly filter cost per unit ($)", min_value=0, value=25, step=5
            )
            no_wo_years = st.slider("Work-order-free years (new units)", 1, 6, 4)
            escalation = st.slider("Annual maintenance cost escalation (%)", 0, 25, 10) / 100

        if len(selected) > 0 and replacement_cost > 0 and current_spend > 0:
            roi = calculate_roi(
                selected, replacement_cost, current_spend,
                filter_cost, no_wo_years, analysis_years=10,
                maintenance_escalation=escalation
            )

            st.divider()

            # ROI Metrics
            r1, r2, r3, r4 = st.columns(4)
            with r1:
                st.metric("Total Replacement Cost", f"${roi['total_replacement_cost']:,.0f}")
            with r2:
                st.metric("First Year Savings", f"${roi['first_year_savings']:,.0f}")
            with r3:
                st.metric("10-Year Total Savings", f"${roi['total_10yr_savings']:,.0f}")
            with r4:
                payback = roi["payback_years"]
                st.metric("Payback Period", f"{payback} yrs" if payback else "N/A")

            # Cost comparison chart
            years = list(range(1, roi["analysis_years"] + 1))
            fig_roi = make_subplots(rows=1, cols=2, subplot_titles=(
                "Annual Cost: Keep vs Replace", "Cumulative Savings vs Investment"
            ))

            fig_roi.add_trace(go.Bar(name="Keep (Old)", x=years, y=roi["old_costs"],
                                     marker_color="#dc3545", opacity=0.8), row=1, col=1)
            fig_roi.add_trace(go.Bar(name="Replace (New)", x=years, y=roi["new_costs"],
                                     marker_color="#28a745", opacity=0.8), row=1, col=1)

            fig_roi.add_trace(go.Scatter(name="Cumulative Savings", x=years, y=roi["cumulative_savings"],
                                         mode="lines+markers", line=dict(color="#0066cc", width=3)), row=1, col=2)
            fig_roi.add_trace(go.Scatter(name="Investment", x=years,
                                         y=[roi["total_replacement_cost"]] * len(years),
                                         mode="lines", line=dict(color="#dc3545", dash="dash")), row=1, col=2)

            fig_roi.update_layout(height=450, barmode="group")
            fig_roi.update_xaxes(title_text="Year", row=1, col=1)
            fig_roi.update_xaxes(title_text="Year", row=1, col=2)
            fig_roi.update_yaxes(title_text="Annual Cost ($)", row=1, col=1)
            fig_roi.update_yaxes(title_text="Dollars ($)", row=1, col=2)
            st.plotly_chart(fig_roi, use_container_width=True)

            # Summary callout
            if payback:
                st.success(f"""
                **ROI Summary:** Replacing {roi['num_units']} units at ${replacement_cost:,.0f} each
                (${roi['total_replacement_cost']:,.0f} total) pays for itself in **{payback} years**.
                Over 10 years, you save **${roi['total_10yr_savings']:,.0f}** compared to keeping the old equipment.
                New units assume ${filter_cost}/quarter filter changes and zero work orders for the first {no_wo_years} years.
                """)
            else:
                st.warning("Payback period exceeds 10 years with current assumptions. Consider adjusting inputs.")

    # ===================== TAB 4: FULL DATA =====================
    with tab4:
        st.subheader("Enriched Asset Data")

        show_cols = st.multiselect(
            "Columns to display",
            options=df.columns.tolist(),
            default=["asset_tag", "brand", "model_no", "serial_no", "facility_name",
                      "asset_type", "condition_clean", "best_mfg_year", "asset_age_years",
                      "age_bucket", "expected_lifespan_years", "life_consumed_pct",
                      "replacement_priority", "replacement_score", "capacity_tons", "mfg_date_source"]
        )
        show_cols = [c for c in show_cols if c in df.columns]

        st.dataframe(filtered[show_cols], use_container_width=True, height=600)

    # ===================== TAB 5: EXPORT =====================
    with tab5:
        st.subheader("Export Enriched Report")

        export_cols = ["asset_tag", "tag_id", "brand", "model_no", "serial_no",
                       "facility_name", "location", "asset_type", "asset_description",
                       "condition_clean", "capacity_tons",
                       "best_mfg_year", "mfg_date_source", "asset_age_years", "age_bucket",
                       "expected_lifespan_years", "life_consumed_pct",
                       "replacement_score", "replacement_priority"]
        export_cols = [c for c in export_cols if c in df.columns]

        export_df = filtered[export_cols].copy()
        export_df.columns = [c.replace("_", " ").title() for c in export_df.columns]

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            export_df.to_excel(writer, sheet_name="Asset Report", index=False)

            # Summary sheet
            summary_data = {
                "Metric": ["Total Assets", "With Manufacture Date", "Average Age (Years)",
                           "Critical Priority", "High Priority", "Medium Priority",
                           "Low Priority", "No Action", "Report Date"],
                "Value": [
                    summary["total_assets"], summary["assets_with_date"],
                    summary["avg_age"],
                    summary.get("priority_breakdown", {}).get("Critical", 0),
                    summary.get("priority_breakdown", {}).get("High", 0),
                    summary.get("priority_breakdown", {}).get("Medium", 0),
                    summary.get("priority_breakdown", {}).get("Low", 0),
                    summary.get("priority_breakdown", {}).get("No Action", 0),
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

        st.download_button(
            "📥 Download Enriched Report (Excel)",
            data=buffer.getvalue(),
            file_name=f"capex_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.caption(f"Export contains {len(export_df)} assets with {len(export_cols)} enriched columns.")
