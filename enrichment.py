"""
Data Enrichment Pipeline
Takes raw asset spreadsheet → identifies missing data → fills what it can →
buckets by age → scores replacement priority.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from serial_decoder import decode_serial, decode_model_number, get_expected_lifespan


def load_and_clean(file) -> pd.DataFrame:
    """Load Excel/CSV, normalize columns, handle messy data."""
    if hasattr(file, 'name') and file.name.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file)

    col_map = {}
    for col in df.columns:
        normalized = col.strip().lower().replace(" ", "_").replace(".", "").replace("(", "").replace(")", "")
        col_map[col] = normalized
    df = df.rename(columns=col_map)

    # Standardize common column names
    rename = {
        "serial_no": "serial_no",
        "model_no": "model_no",
        "manufactured_date": "manufactured_date",
        "install_date": "install_date",
        "asset_type": "asset_type",
        "brand": "brand",
        "facility_name": "facility_name",
        "location": "location",
        "condition": "condition",
        "capacity": "capacity",
        "energy_efficiency": "energy_efficiency",
        "life_expectancy": "life_expectancy",
        "asset_description": "asset_description",
        "tag_id": "tag_id",
        "asset_tag": "asset_tag",
        "age_months": "age_months",
    }

    existing_renames = {k: v for k, v in rename.items() if k in df.columns}
    df = df.rename(columns=existing_renames)

    return df


def parse_capacity(val) -> float:
    """Parse capacity strings like '5 ton', '120000 BTU' into tons."""
    if pd.isna(val):
        return np.nan
    val = str(val).strip().upper()
    if val in ("FALSE", "NAN", "", "ENERGY EFFICIENCY"):
        return np.nan
    ton_match = pd.Series([val]).str.extract(r'([\d.]+)\s*TON', expand=False)
    if ton_match.notna().iloc[0]:
        return float(ton_match.iloc[0])
    btu_match = pd.Series([val]).str.extract(r'([\d.]+)\s*BTU', expand=False)
    if btu_match.notna().iloc[0]:
        return float(btu_match.iloc[0]) / 12000
    kw_match = pd.Series([val]).str.extract(r'([\d.]+)\s*KW', expand=False)
    if kw_match.notna().iloc[0]:
        return np.nan  # KW is for generators, not comparable
    try:
        return float(val)
    except (ValueError, TypeError):
        return np.nan


def clean_condition(val) -> str:
    """Normalize condition field, filtering out garbage data."""
    if pd.isna(val):
        return "unknown"
    val = str(val).strip().lower()
    valid = {"excellent", "good", "average", "poor", "broken"}
    if val in valid:
        return val
    return "unknown"


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Main enrichment: decode serials, fill dates, add lifespan, bucket by age."""
    today = datetime.now()

    # --- Step 1: Clean existing data ---
    if "condition" in df.columns:
        df["condition_clean"] = df["condition"].apply(clean_condition)
    else:
        df["condition_clean"] = "unknown"

    if "capacity" in df.columns:
        df["capacity_tons"] = df["capacity"].apply(parse_capacity)
    else:
        df["capacity_tons"] = np.nan

    # --- Step 2: Decode serial numbers ---
    decoded_years = []
    decoded_months = []
    decoded_capacity = []

    for _, row in df.iterrows():
        serial = row.get("serial_no", "")
        brand = row.get("brand", "")
        model = row.get("model_no", "")

        info = decode_serial(str(serial) if pd.notna(serial) else "",
                             str(brand) if pd.notna(brand) else "",
                             str(model) if pd.notna(model) else "")
        decoded_years.append(info.get("manufacture_year"))
        decoded_months.append(info.get("manufacture_month"))
        decoded_capacity.append(info.get("capacity_tons"))

    df["decoded_year"] = decoded_years
    df["decoded_month"] = decoded_months
    df["decoded_capacity_tons"] = decoded_capacity

    # --- Step 3: Build best available manufacture date ---
    # Priority: existing Manufactured Date > decoded serial > Install Date
    df["manufactured_date_parsed"] = pd.to_datetime(df.get("manufactured_date"), errors="coerce")
    df["install_date_parsed"] = pd.to_datetime(df.get("install_date"), errors="coerce")

    df["best_mfg_year"] = np.nan

    # Use existing manufactured date first
    mask = df["manufactured_date_parsed"].notna()
    df.loc[mask, "best_mfg_year"] = df.loc[mask, "manufactured_date_parsed"].dt.year

    # Fill with decoded serial year
    mask = df["best_mfg_year"].isna() & df["decoded_year"].notna()
    df.loc[mask, "best_mfg_year"] = df.loc[mask, "decoded_year"]

    # Fill with install date (assume manufactured ~1 year before install)
    mask = df["best_mfg_year"].isna() & df["install_date_parsed"].notna()
    df.loc[mask, "best_mfg_year"] = df.loc[mask, "install_date_parsed"].dt.year - 1

    # --- Step 4: Calculate age ---
    current_year = today.year
    df["asset_age_years"] = current_year - df["best_mfg_year"]
    df.loc[df["asset_age_years"] < 0, "asset_age_years"] = np.nan

    # --- Step 5: Age buckets ---
    df["age_bucket"] = df["asset_age_years"].apply(_assign_bucket)

    # --- Step 6: Expected lifespan ---
    df["expected_lifespan_years"] = df.apply(
        lambda r: get_expected_lifespan(
            str(r.get("asset_type", "")) if pd.notna(r.get("asset_type")) else "",
            str(r.get("asset_description", "")) if pd.notna(r.get("asset_description")) else ""
        ), axis=1
    )

    # --- Step 7: Life consumed % ---
    df["life_consumed_pct"] = (df["asset_age_years"] / df["expected_lifespan_years"] * 100).round(1)
    df.loc[df["life_consumed_pct"] > 200, "life_consumed_pct"] = 200  # Cap at 200%

    # --- Step 8: Replacement priority score ---
    df["replacement_score"] = df.apply(_calc_replacement_score, axis=1)
    df["replacement_priority"] = df["replacement_score"].apply(_score_to_priority)

    # --- Step 9: Fill capacity from decoded model if missing ---
    mask = df["capacity_tons"].isna() & df["decoded_capacity_tons"].notna()
    df.loc[mask, "capacity_tons"] = df.loc[mask, "decoded_capacity_tons"]

    # --- Step 10: Data source tracking ---
    df["mfg_date_source"] = "Unknown"
    mask1 = df["manufactured_date_parsed"].notna()
    df.loc[mask1, "mfg_date_source"] = "Original Data"
    mask2 = ~mask1 & df["decoded_year"].notna()
    df.loc[mask2, "mfg_date_source"] = "Serial Decode"
    mask3 = ~mask1 & ~mask2 & df["install_date_parsed"].notna()
    df.loc[mask3, "mfg_date_source"] = "Install Date (est.)"

    return df


def _assign_bucket(age) -> str:
    """Assign age bucket string."""
    if pd.isna(age) or age < 0:
        return "Unknown"
    age = int(age)
    if age <= 0:
        return "< 1 Year"
    elif age <= 30:
        return f"{age} Years"
    else:
        return "30+ Years"


def _calc_replacement_score(row) -> float:
    """Score 0-100. Higher = more urgent replacement needed."""
    score = 0.0
    life_pct = row.get("life_consumed_pct", 0)
    condition = row.get("condition_clean", "unknown")
    age = row.get("asset_age_years", 0)

    if pd.notna(life_pct):
        if life_pct >= 100:
            score += 50
        elif life_pct >= 80:
            score += 35
        elif life_pct >= 60:
            score += 20
        elif life_pct >= 40:
            score += 10

    condition_scores = {"broken": 30, "poor": 20, "average": 10, "good": 5, "excellent": 0, "unknown": 8}
    score += condition_scores.get(condition, 8)

    if pd.notna(age):
        if age >= 25:
            score += 20
        elif age >= 20:
            score += 15
        elif age >= 15:
            score += 10
        elif age >= 10:
            score += 5

    return min(score, 100)


def _score_to_priority(score) -> str:
    if pd.isna(score):
        return "Unknown"
    if score >= 70:
        return "Critical"
    elif score >= 50:
        return "High"
    elif score >= 30:
        return "Medium"
    elif score >= 15:
        return "Low"
    else:
        return "No Action"


def calculate_roi(selected_assets: pd.DataFrame, replacement_cost_per_unit: float,
                  current_annual_spend: float, quarterly_filter_cost: float = 25.0,
                  no_wo_years: int = 4, analysis_years: int = 10,
                  maintenance_escalation: float = 0.10) -> dict:
    """Calculate ROI for replacing selected assets.

    Args:
        selected_assets: DataFrame of assets to replace
        replacement_cost_per_unit: Cost per new unit
        current_annual_spend: Current total annual maintenance spend on these units
        quarterly_filter_cost: Per-unit quarterly filter cost ($25 default)
        no_wo_years: Years with zero work orders on new units (4 default)
        analysis_years: How many years to project (10 default)
        maintenance_escalation: Annual increase in maintenance costs for old units (10% default)
    """
    num_units = len(selected_assets)
    total_replacement_cost = num_units * replacement_cost_per_unit
    annual_filter_cost = num_units * quarterly_filter_cost * 4

    # Projected costs: old vs new over analysis period
    old_costs = []
    new_costs = []
    cumulative_savings = []
    running_savings = 0

    for year in range(1, analysis_years + 1):
        # Old equipment: costs escalate
        old_annual = current_annual_spend * ((1 + maintenance_escalation) ** (year - 1))
        old_costs.append(old_annual)

        # New equipment: filters only for first N years, then modest maintenance
        if year <= no_wo_years:
            new_annual = annual_filter_cost
        else:
            # After warranty period, modest maintenance begins
            base_maintenance = current_annual_spend * 0.15  # 15% of original spend
            years_post_warranty = year - no_wo_years
            new_annual = annual_filter_cost + base_maintenance * ((1 + 0.05) ** (years_post_warranty - 1))
        new_costs.append(new_annual)

        running_savings += (old_annual - new_annual)
        cumulative_savings.append(running_savings)

    # Simple payback
    payback_year = None
    for i, savings in enumerate(cumulative_savings):
        if savings >= total_replacement_cost:
            # Interpolate
            if i == 0:
                payback_year = total_replacement_cost / (cumulative_savings[0]) if cumulative_savings[0] > 0 else None
            else:
                prev = cumulative_savings[i-1]
                diff = savings - prev
                fraction = (total_replacement_cost - prev) / diff if diff > 0 else 0
                payback_year = i + fraction
            break

    return {
        "num_units": num_units,
        "total_replacement_cost": total_replacement_cost,
        "current_annual_spend": current_annual_spend,
        "annual_filter_cost_new": annual_filter_cost,
        "first_year_savings": old_costs[0] - new_costs[0],
        "total_10yr_savings": sum(old_costs) - sum(new_costs),
        "payback_years": round(payback_year, 1) if payback_year else None,
        "old_costs": old_costs,
        "new_costs": new_costs,
        "cumulative_savings": cumulative_savings,
        "analysis_years": analysis_years,
    }


def get_enrichment_summary(df: pd.DataFrame) -> dict:
    """Return summary stats for the dashboard."""
    total = len(df)
    dated = df["best_mfg_year"].notna().sum()
    return {
        "total_assets": total,
        "assets_with_date": int(dated),
        "assets_without_date": int(total - dated),
        "date_coverage_pct": round(dated / total * 100, 1) if total > 0 else 0,
        "source_breakdown": df["mfg_date_source"].value_counts().to_dict(),
        "priority_breakdown": df["replacement_priority"].value_counts().to_dict() if "replacement_priority" in df.columns else {},
        "avg_age": round(df["asset_age_years"].mean(), 1) if df["asset_age_years"].notna().any() else None,
        "brands_count": df["brand"].nunique() if "brand" in df.columns else 0,
        "facilities_count": df["facility_name"].nunique() if "facility_name" in df.columns else 0,
    }
