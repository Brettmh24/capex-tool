"""
Natural Language Query Engine for Asset Data
Parses plain English questions and returns answers from the enriched dataframe.
No external API needed — uses pattern matching and pandas operations.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime


def process_query(query: str, df: pd.DataFrame) -> dict:
    """Process a natural language query against the asset dataframe.
    Returns dict with 'answer' (str), 'data' (DataFrame or None), 'chart_type' (str or None)."""

    q = query.lower().strip()
    result = {"answer": "", "data": None, "chart_type": None}

    # --- Try each query pattern ---
    # Count queries
    r = _try_count_query(q, df)
    if r: return r

    # Oldest/newest queries
    r = _try_age_extreme_query(q, df)
    if r: return r

    # Brand breakdown
    r = _try_brand_query(q, df)
    if r: return r

    # Facility queries
    r = _try_facility_query(q, df)
    if r: return r

    # Priority queries
    r = _try_priority_query(q, df)
    if r: return r

    # Condition queries
    r = _try_condition_query(q, df)
    if r: return r

    # Age-related queries
    r = _try_age_query(q, df)
    if r: return r

    # Capacity queries
    r = _try_capacity_query(q, df)
    if r: return r

    # Cost/ROI queries
    r = _try_cost_query(q, df)
    if r: return r

    # Summary / overview
    r = _try_summary_query(q, df)
    if r: return r

    # Show/list/find queries (generic filter)
    r = _try_show_query(q, df)
    if r: return r

    # Fallback: try to find any matching data
    r = _try_fuzzy_match(q, df)
    if r: return r

    return {
        "answer": "I couldn't understand that query. Try asking things like:\n"
                  "- **How many assets are critical?**\n"
                  "- **Show me all Trane units over 15 years old**\n"
                  "- **What's the oldest equipment in Chicago?**\n"
                  "- **How many assets by brand?**\n"
                  "- **Which facilities have the most critical assets?**\n"
                  "- **Show me all poor condition units**\n"
                  "- **What's the average age of Carrier units?**\n"
                  "- **Summary of all assets**",
        "data": None,
        "chart_type": None
    }


def _extract_number(q: str) -> int:
    """Extract first number from query string."""
    match = re.search(r'\b(\d+)\b', q)
    return int(match.group(1)) if match else None


def _extract_brand(q: str, df: pd.DataFrame) -> str:
    """Find a brand name mentioned in the query."""
    if "brand" not in df.columns:
        return None
    brands = df["brand"].dropna().unique()
    for brand in brands:
        if str(brand).lower() in q:
            return brand
    return None


def _extract_facility(q: str, df: pd.DataFrame) -> str:
    """Find a facility name mentioned in the query."""
    for col in ["facility_name", "location"]:
        if col not in df.columns:
            continue
        facilities = df[col].dropna().unique()
        for fac in facilities:
            fac_lower = str(fac).lower()
            # Check if any significant part of facility name is in query
            parts = fac_lower.replace(",", "").split()
            for part in parts:
                if len(part) > 2 and part in q and part not in ("the", "and", "for", "all", "are", "how", "many", "what"):
                    return fac
    return None


def _extract_priority(q: str) -> str:
    """Extract priority level from query."""
    if "critical" in q: return "Critical"
    if "high" in q: return "High"
    if "medium" in q: return "Medium"
    if "low" in q: return "Low"
    if "no action" in q: return "No Action"
    return None


def _extract_condition(q: str) -> str:
    """Extract condition from query."""
    if "broken" in q: return "broken"
    if "poor" in q: return "poor"
    if "average" in q: return "average"
    if "good" in q and "no" not in q: return "good"
    if "excellent" in q: return "excellent"
    return None


def _display_cols(df):
    """Standard display columns for result tables."""
    cols = ["asset_tag", "brand", "model_no", "facility_name", "asset_type",
            "condition_clean", "best_mfg_year", "asset_age_years",
            "replacement_priority", "replacement_score", "capacity_tons"]
    return [c for c in cols if c in df.columns]


# --- QUERY HANDLERS ---

def _try_count_query(q, df):
    """Handle: how many, count, total, number of"""
    if not any(w in q for w in ["how many", "count", "total number", "number of"]):
        return None

    subset = df.copy()
    brand = _extract_brand(q, df)
    facility = _extract_facility(q, df)
    priority = _extract_priority(q)
    condition = _extract_condition(q)
    num = _extract_number(q)

    filters_desc = []

    if brand:
        subset = subset[subset["brand"].str.lower() == brand.lower()]
        filters_desc.append(f"**{brand}**")
    if facility:
        for col in ["facility_name", "location"]:
            if col in subset.columns:
                mask = subset[col].str.lower().str.contains(facility.lower(), na=False)
                if mask.any():
                    subset = subset[mask]
                    filters_desc.append(f"at **{facility}**")
                    break
    if priority:
        subset = subset[subset["replacement_priority"] == priority]
        filters_desc.append(f"with **{priority}** priority")
    if condition:
        subset = subset[subset["condition_clean"] == condition]
        filters_desc.append(f"in **{condition}** condition")

    if num and any(w in q for w in ["over", "above", "older", "more than", "greater", "exceed", "past"]):
        if "asset_age_years" in subset.columns:
            subset = subset[subset["asset_age_years"] > num]
            filters_desc.append(f"over **{num} years** old")
    elif num and any(w in q for w in ["under", "below", "younger", "less than", "fewer", "within"]):
        if "asset_age_years" in subset.columns:
            subset = subset[subset["asset_age_years"] < num]
            filters_desc.append(f"under **{num} years** old")

    if "asset_type" in q or "type" in q:
        if "asset_type" in df.columns:
            type_counts = subset["asset_type"].value_counts()
            answer = f"**{len(subset):,}** assets " + " ".join(filters_desc) + " broken down by type:\n\n"
            for t, c in type_counts.items():
                answer += f"- {t}: **{c}**\n"
            return {"answer": answer, "data": None, "chart_type": None}

    filter_text = " ".join(filters_desc) if filters_desc else "total"
    answer = f"**{len(subset):,}** assets {filter_text}."

    if len(subset) > 0 and len(subset) <= 100:
        return {"answer": answer, "data": subset[_display_cols(subset)], "chart_type": None}
    elif len(subset) > 100:
        return {"answer": answer + f" (Showing first 50 of {len(subset)})", "data": subset[_display_cols(subset)].head(50), "chart_type": None}
    return {"answer": answer, "data": None, "chart_type": None}


def _try_age_extreme_query(q, df):
    """Handle: oldest, newest, youngest"""
    if "asset_age_years" not in df.columns:
        return None

    is_oldest = any(w in q for w in ["oldest", "most aged", "longest"])
    is_newest = any(w in q for w in ["newest", "youngest", "most recent", "latest"])

    if not is_oldest and not is_newest:
        return None

    subset = df.dropna(subset=["asset_age_years"]).copy()
    brand = _extract_brand(q, df)
    facility = _extract_facility(q, df)
    num = _extract_number(q) or 10

    if brand:
        subset = subset[subset["brand"].str.lower() == brand.lower()]
    if facility:
        for col in ["facility_name", "location"]:
            if col in subset.columns:
                mask = subset[col].str.lower().str.contains(facility.lower(), na=False)
                if mask.any():
                    subset = subset[mask]
                    break

    if is_oldest:
        subset = subset.nlargest(num, "asset_age_years")
        label = "oldest"
    else:
        subset = subset.nsmallest(num, "asset_age_years")
        label = "newest"

    answer = f"**Top {len(subset)} {label} assets:**"
    return {"answer": answer, "data": subset[_display_cols(subset)], "chart_type": None}


def _try_brand_query(q, df):
    """Handle: by brand, brand breakdown, brand summary"""
    if "brand" not in df.columns:
        return None
    if not any(w in q for w in ["by brand", "brand breakdown", "brand summary", "each brand", "per brand", "brands"]):
        return None

    brand_stats = df.groupby("brand").agg(
        count=("brand", "size"),
        avg_age=("asset_age_years", "mean"),
        critical=("replacement_priority", lambda x: (x == "Critical").sum()),
        high=("replacement_priority", lambda x: (x == "High").sum()),
    ).round(1).sort_values("count", ascending=False)

    answer = f"**{len(brand_stats)} brands** across {len(df):,} assets. Top brands:"
    return {"answer": answer, "data": brand_stats.head(20), "chart_type": "bar_brand"}


def _try_facility_query(q, df):
    """Handle: by facility, facility breakdown, which facilities"""
    if "facility_name" not in df.columns:
        return None

    specific_facility = _extract_facility(q, df)

    if specific_facility:
        subset = df.copy()
        for col in ["facility_name", "location"]:
            if col in subset.columns:
                mask = subset[col].str.lower().str.contains(specific_facility.lower(), na=False)
                if mask.any():
                    subset = subset[mask]
                    break

        total = len(subset)
        avg_age = subset["asset_age_years"].mean()
        critical = (subset["replacement_priority"] == "Critical").sum()
        high = (subset["replacement_priority"] == "High").sum()

        answer = (f"**{specific_facility}**: {total} assets, avg age {avg_age:.1f} years, "
                  f"{critical} critical, {high} high priority.")
        return {"answer": answer, "data": subset[_display_cols(subset)].head(50), "chart_type": None}

    if not any(w in q for w in ["by facility", "facility breakdown", "which facility", "facilities",
                                  "by location", "each facility", "per facility", "site"]):
        return None

    fac_stats = df.groupby("facility_name").agg(
        count=("facility_name", "size"),
        avg_age=("asset_age_years", "mean"),
        critical=("replacement_priority", lambda x: (x == "Critical").sum()),
    ).round(1).sort_values("count", ascending=False)

    answer = f"**{len(fac_stats)} facilities**. Top facilities by asset count:"
    return {"answer": answer, "data": fac_stats.head(20), "chart_type": "bar_facility"}


def _try_priority_query(q, df):
    """Handle: critical assets, high priority, what should I replace"""
    priority = _extract_priority(q)
    if not priority and not any(w in q for w in ["replace", "priority", "urgent", "attention"]):
        return None

    if not priority:
        if any(w in q for w in ["replace first", "most urgent", "should i replace", "need to replace", "attention"]):
            priority = "Critical"
        else:
            # Show all priority breakdown
            counts = df["replacement_priority"].value_counts()
            answer = "**Replacement Priority Breakdown:**\n\n"
            for p in ["Critical", "High", "Medium", "Low", "No Action", "Unknown"]:
                if p in counts:
                    answer += f"- {p}: **{counts[p]}**\n"
            return {"answer": answer, "data": None, "chart_type": None}

    subset = df[df["replacement_priority"] == priority]
    answer = f"**{len(subset):,}** assets with **{priority}** priority."
    if len(subset) > 50:
        answer += f" (Showing first 50)"
    return {"answer": answer, "data": subset[_display_cols(subset)].head(50), "chart_type": None}


def _try_condition_query(q, df):
    """Handle: poor condition, broken, condition breakdown"""
    if "condition_clean" not in df.columns:
        return None

    condition = _extract_condition(q)

    if not condition and any(w in q for w in ["condition breakdown", "by condition", "condition summary", "conditions"]):
        counts = df["condition_clean"].value_counts()
        answer = "**Condition Breakdown:**\n\n"
        for c, n in counts.items():
            answer += f"- {c.title()}: **{n}**\n"
        return {"answer": answer, "data": None, "chart_type": None}

    if not condition:
        return None

    subset = df[df["condition_clean"] == condition]
    answer = f"**{len(subset):,}** assets in **{condition}** condition."
    return {"answer": answer, "data": subset[_display_cols(subset)].head(50), "chart_type": None}


def _try_age_query(q, df):
    """Handle: average age, age of, how old"""
    if "asset_age_years" not in df.columns:
        return None

    if not any(w in q for w in ["average age", "avg age", "mean age", "how old", "age of"]):
        return None

    subset = df.dropna(subset=["asset_age_years"])
    brand = _extract_brand(q, df)
    facility = _extract_facility(q, df)

    label_parts = []
    if brand:
        subset = subset[subset["brand"].str.lower() == brand.lower()]
        label_parts.append(f"**{brand}**")
    if facility:
        for col in ["facility_name", "location"]:
            if col in subset.columns:
                mask = subset[col].str.lower().str.contains(facility.lower(), na=False)
                if mask.any():
                    subset = subset[mask]
                    label_parts.append(f"at **{facility}**")
                    break

    if len(subset) == 0:
        return {"answer": "No assets found with age data for that filter.", "data": None, "chart_type": None}

    avg = subset["asset_age_years"].mean()
    median = subset["asset_age_years"].median()
    oldest = subset["asset_age_years"].max()
    newest = subset["asset_age_years"].min()
    label = " ".join(label_parts) if label_parts else "all assets"

    answer = (f"**Age stats for {label}** ({len(subset)} assets):\n\n"
              f"- Average: **{avg:.1f} years**\n"
              f"- Median: **{median:.0f} years**\n"
              f"- Oldest: **{oldest:.0f} years**\n"
              f"- Newest: **{newest:.0f} years**")

    return {"answer": answer, "data": None, "chart_type": None}


def _try_capacity_query(q, df):
    """Handle: capacity, tonnage, tons"""
    if "capacity_tons" not in df.columns:
        return None
    if not any(w in q for w in ["capacity", "tonnage", "tons", "ton"]):
        return None

    subset = df.dropna(subset=["capacity_tons"])
    total_tons = subset["capacity_tons"].sum()
    avg_tons = subset["capacity_tons"].mean()

    answer = (f"**Capacity Summary** ({len(subset)} assets with data):\n\n"
              f"- Total: **{total_tons:,.1f} tons**\n"
              f"- Average per unit: **{avg_tons:.1f} tons**\n\n"
              f"**By size:**\n")

    size_bins = [(0, 3, "Small (< 3 ton)"), (3, 5, "Medium (3-5 ton)"),
                 (5, 10, "Large (5-10 ton)"), (10, 50, "XL (10-50 ton)"), (50, 9999, "Industrial (50+ ton)")]
    for lo, hi, label in size_bins:
        count = len(subset[(subset["capacity_tons"] >= lo) & (subset["capacity_tons"] < hi)])
        if count > 0:
            answer += f"- {label}: **{count}**\n"

    return {"answer": answer, "data": None, "chart_type": None}


def _try_cost_query(q, df):
    """Handle: cost to replace, replacement cost, how much"""
    if not any(w in q for w in ["cost", "how much", "spend", "investment", "budget"]):
        return None

    priority = _extract_priority(q)
    if priority:
        subset = df[df["replacement_priority"] == priority]
    elif any(w in q for w in ["all critical", "critical and high"]):
        subset = df[df["replacement_priority"].isin(["Critical", "High"])]
    elif "all" in q:
        subset = df[df["replacement_priority"].isin(["Critical", "High", "Medium"])]
    else:
        subset = df[df["replacement_priority"] == "Critical"]

    count = len(subset)
    estimates = {
        "HVAC UNIT": 8500,
        "Generator": 25000,
        "Ice Machine": 5000,
        "Lift": 15000,
        "Air Bag System": 3000,
        "Grinder": 8000,
        "Shop Air Compressor": 12000,
    }

    total_est = 0
    for _, row in subset.iterrows():
        asset_type = row.get("asset_type", "HVAC UNIT")
        total_est += estimates.get(str(asset_type), 8500)

    answer = (f"**Estimated replacement cost for {count} assets:**\n\n"
              f"- Total estimated: **${total_est:,.0f}**\n"
              f"- Average per unit: **${total_est/count:,.0f}**\n\n"
              f"*Note: These are rough estimates. Actual costs vary by model, capacity, and installation.*")

    return {"answer": answer, "data": None, "chart_type": None}


def _try_summary_query(q, df):
    """Handle: summary, overview, tell me about, describe"""
    if not any(w in q for w in ["summary", "overview", "tell me about", "describe", "give me a",
                                  "what do i have", "what's in", "break down everything"]):
        return None

    total = len(df)
    with_age = df["asset_age_years"].notna().sum()
    avg_age = df["asset_age_years"].mean()
    brands = df["brand"].nunique() if "brand" in df.columns else 0
    facilities = df["facility_name"].nunique() if "facility_name" in df.columns else 0

    types = df["asset_type"].value_counts() if "asset_type" in df.columns else pd.Series()
    priorities = df["replacement_priority"].value_counts() if "replacement_priority" in df.columns else pd.Series()
    conditions = df["condition_clean"].value_counts() if "condition_clean" in df.columns else pd.Series()

    answer = f"**Asset Portfolio Summary:**\n\n"
    answer += f"- **{total:,}** total assets across **{facilities}** facilities\n"
    answer += f"- **{brands}** different brands\n"
    answer += f"- Average age: **{avg_age:.1f} years** ({with_age} assets with age data)\n\n"

    answer += "**By Type:**\n"
    for t, c in types.head(5).items():
        answer += f"- {t}: {c}\n"

    answer += "\n**By Priority:**\n"
    for p in ["Critical", "High", "Medium", "Low", "No Action"]:
        if p in priorities:
            answer += f"- {p}: {priorities[p]}\n"

    answer += "\n**By Condition:**\n"
    for c in ["excellent", "good", "average", "poor", "broken"]:
        if c in conditions:
            answer += f"- {c.title()}: {conditions[c]}\n"

    return {"answer": answer, "data": None, "chart_type": None}


def _try_show_query(q, df):
    """Handle: show me, list, find, get"""
    if not any(w in q for w in ["show", "list", "find", "get", "display", "which", "what are"]):
        return None

    subset = df.copy()
    filters_desc = []
    brand = _extract_brand(q, df)
    facility = _extract_facility(q, df)
    priority = _extract_priority(q)
    condition = _extract_condition(q)
    num = _extract_number(q)

    if brand:
        subset = subset[subset["brand"].str.lower() == brand.lower()]
        filters_desc.append(f"brand: **{brand}**")
    if facility:
        for col in ["facility_name", "location"]:
            if col in subset.columns:
                mask = subset[col].str.lower().str.contains(facility.lower(), na=False)
                if mask.any():
                    subset = subset[mask]
                    filters_desc.append(f"facility: **{facility}**")
                    break
    if priority:
        subset = subset[subset["replacement_priority"] == priority]
        filters_desc.append(f"priority: **{priority}**")
    if condition:
        subset = subset[subset["condition_clean"] == condition]
        filters_desc.append(f"condition: **{condition}**")

    if num:
        if any(w in q for w in ["over", "above", "older", "more than", "greater", "past"]):
            subset = subset[subset["asset_age_years"] > num]
            filters_desc.append(f"over **{num} years** old")
        elif any(w in q for w in ["under", "below", "younger", "less than"]):
            subset = subset[subset["asset_age_years"] < num]
            filters_desc.append(f"under **{num} years** old")

    if not filters_desc:
        return None

    filter_text = ", ".join(filters_desc)
    answer = f"**{len(subset):,}** assets matching {filter_text}."
    if len(subset) > 50:
        answer += " (Showing first 50)"

    return {"answer": answer, "data": subset[_display_cols(subset)].head(50), "chart_type": None}


def _try_fuzzy_match(q, df):
    """Last resort: search all text columns for any matching terms."""
    words = [w for w in q.split() if len(w) > 3 and w not in
             ("show", "what", "many", "have", "that", "with", "from", "about", "tell", "give",
              "list", "find", "there", "which", "where", "does", "they", "them", "this", "these")]

    if not words:
        return None

    mask = pd.Series([False] * len(df))
    text_cols = ["brand", "model_no", "facility_name", "asset_description", "asset_type", "asset_tag"]
    text_cols = [c for c in text_cols if c in df.columns]

    for word in words:
        word_mask = pd.Series([False] * len(df))
        for col in text_cols:
            word_mask = word_mask | df[col].astype(str).str.lower().str.contains(word, na=False)
        mask = mask | word_mask

    subset = df[mask]
    if len(subset) == 0:
        return None

    answer = f"Found **{len(subset):,}** assets matching your search."
    if len(subset) > 50:
        answer += " (Showing first 50)"
    return {"answer": answer, "data": subset[_display_cols(subset)].head(50), "chart_type": None}
