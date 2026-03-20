from flask import Flask, render_template, request, redirect, url_for, session, jsonify, make_response
from flask_cors import CORS
from flask_compress import Compress
import os
import io
import csv
import importlib
import re
from functools import lru_cache
import pandas as pd
from datetime import timedelta, datetime
from werkzeug.utils import secure_filename


def load_env_file(path=".env"):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            # Keep real environment variables as highest priority.
            os.environ.setdefault(key, value)


load_env_file()

app = Flask(__name__)
Compress(app)
CORS(
    app,
    supports_credentials=True,
    resources={
        r"/*": {
            "origins": [
                r"http://localhost(:\\d+)?",
                r"http://127\.0\.0\.1(:\\d+)?"
            ]
        }
    }
)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret-key")
app.permanent_session_lifetime = timedelta(days=30)

# Apply secure cookie defaults in hosted production.
IS_PRODUCTION = (
    os.environ.get("APP_ENV", "").strip().lower() == "production"
    or os.environ.get("FLASK_ENV", "").strip().lower() == "production"
    or os.environ.get("RENDER", "").strip().lower() == "true"
)

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")
app.config["SESSION_COOKIE_SECURE"] = IS_PRODUCTION
app.config["PREFERRED_URL_SCHEME"] = "https" if IS_PRODUCTION else "http"


@app.template_filter("fmt_num")
def fmt_num(value):
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return value

    if numeric_value.is_integer():
        return f"{int(numeric_value):,}"

    return f"{numeric_value:,.2f}"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "admin123")
REACT_LOGIN_URL = os.environ.get("REACT_LOGIN_URL", "").strip()

df_global = None
charts_global = None
results_global = None
selected_window_global = "all"
selected_product_global = "all"
selected_company_global = "all"
latest_upload_path_global = None
secondary_upload_path_global = None
comparison_results_global = None
comparison_charts_global = None
selected_compare_columns_global = ["Count", "ValueNett"]
selected_compare_category_global = "Product"
selected_compare_items_global = []
selected_compare_item_scope_global = "both"
storytelling_qa_history_global = []
auto_corrections_primary_global = 0
auto_corrections_secondary_global = 0

BLOCK01_SORT_OPTIONS = {"highest", "least", "all"}
BLOCK01_METRIC_OPTIONS = ["Count", "ValueNett"]
ALLOWED_CURRENCY_CODES = {"USD", "ZWG", "ZIG", "ZWL"}
CURRENCY_DISPLAY_ORDER = ["USD", "ZWG", "ZIG", "ZWL"]

_analyzer_module = None


def get_analyzer_module():
    global _analyzer_module

    if _analyzer_module is None:
        _analyzer_module = importlib.import_module("analyzer")

    return _analyzer_module


def _file_cache_key(filepath):
    absolute_path = os.path.abspath(filepath)
    stat_result = os.stat(absolute_path)
    return absolute_path, stat_result.st_mtime_ns, stat_result.st_size


@lru_cache(maxsize=16)
def _read_tabular_file_cached(filepath, modified_time_ns, file_size):
    del modified_time_ns, file_size
    return get_analyzer_module().read_tabular_file(filepath)


def read_tabular_file(filepath):
    cache_key = _file_cache_key(filepath)
    cached_df = _read_tabular_file_cached(*cache_key)
    return cached_df.copy(deep=True)


@lru_cache(maxsize=32)
def _load_filter_options_cached(filepath, modified_time_ns, file_size):
    source_df = _read_tabular_file_cached(filepath, modified_time_ns, file_size)
    product_options = tuple(sorted(source_df["Product"].dropna().astype(str).unique())) if "Product" in source_df.columns else ()
    company_options = tuple(sorted(source_df["CompanyName"].dropna().astype(str).unique())) if "CompanyName" in source_df.columns else ()
    return product_options, company_options


@lru_cache(maxsize=32)
def _load_comparison_options_cached(filepath_a, modified_time_ns_a, file_size_a, filepath_b, modified_time_ns_b, file_size_b):
    df_a = _read_tabular_file_cached(filepath_a, modified_time_ns_a, file_size_a)
    df_b = _read_tabular_file_cached(filepath_b, modified_time_ns_b, file_size_b)

    common_columns = sorted(set(df_a.columns).intersection(set(df_b.columns)))

    numeric_candidate_columns = []
    for column_name in common_columns:
        series_a = df_a[column_name]
        series_b = df_b[column_name]
        if pd.to_numeric(series_a, errors="coerce").notna().any() and pd.to_numeric(series_b, errors="coerce").notna().any():
            numeric_candidate_columns.append(column_name)

    preferred_compare_columns = [
        column_name for column_name in ["Count", "ValueNett"] if column_name in numeric_candidate_columns
    ]
    available_compare_columns = tuple(preferred_compare_columns if preferred_compare_columns else numeric_candidate_columns)

    preferred_categories = [
        column_name for column_name in ["Product", "CompanyName"] if column_name in common_columns
    ]
    available_categories = tuple(preferred_categories if preferred_categories else common_columns)

    return available_compare_columns, available_categories


@lru_cache(maxsize=32)
def _build_block01_rows_cached(
    filepath,
    modified_time_ns,
    file_size,
    sort_order="all",
    ranking_metric="Count",
    top_n="all",
    anomalies_only=False,
):
    source_df = _read_tabular_file_cached(filepath, modified_time_ns, file_size)
    if source_df.empty:
        return (), "all", "Count", "all", False

    normalized_sort_order = str(sort_order or "all").strip().lower()
    if normalized_sort_order not in BLOCK01_SORT_OPTIONS:
        normalized_sort_order = "all"

    metric_lookup = {"count": "Count", "valuenett": "ValueNett"}
    requested_metric = metric_lookup.get(str(ranking_metric or "Count").strip().lower(), "Count")

    lower_columns = {col.lower(): col for col in source_df.columns}

    def resolve_column(candidates):
        for candidate in candidates:
            if candidate in source_df.columns:
                return candidate
            matched = lower_columns.get(candidate.lower())
            if matched:
                return matched
        return None

    app_description_col = resolve_column(["AppDescription", "App Description", "Company|AppDescription", "CompanyName"])
    product_col = resolve_column(["Product"])
    count_col = resolve_column(["Count"])
    value_nett_col = resolve_column(["ValueNett", "Value Nett"])
    value_debit_col = resolve_column(["ValueDebit", "Value Debit"])

    block01_df = pd.DataFrame(index=source_df.index)
    block01_df["app_description"] = (
        source_df[app_description_col].fillna("").astype(str).str.strip() if app_description_col else ""
    )
    block01_df["product"] = source_df[product_col].fillna("").astype(str).str.strip() if product_col else ""
    block01_df["count"] = pd.to_numeric(source_df[count_col], errors="coerce").fillna(0) if count_col else 0
    block01_df["valuenett"] = pd.to_numeric(source_df[value_nett_col], errors="coerce").fillna(0) if value_nett_col else 0
    block01_df["valuedebit"] = pd.to_numeric(source_df[value_debit_col], errors="coerce").fillna(0) if value_debit_col else 0

    block01_df = block01_df[
        (block01_df["app_description"] != "")
        | (block01_df["product"] != "")
        | (block01_df["count"] != 0)
        | (block01_df["valuenett"] != 0)
        | (block01_df["valuedebit"] != 0)
    ]

    if block01_df.empty:
        return (), normalized_sort_order, requested_metric, "all", False

    metric_sort_column = "count" if requested_metric == "Count" else "valuenett"
    if requested_metric == "Count" and count_col is None and value_nett_col is not None:
        requested_metric = "ValueNett"
        metric_sort_column = "valuenett"
    elif requested_metric == "ValueNett" and value_nett_col is None and count_col is not None:
        requested_metric = "Count"
        metric_sort_column = "count"

    normalized_top_n = str(top_n or "all").strip().lower()
    if normalized_top_n != "all":
        try:
            normalized_top_n = str(max(1, int(normalized_top_n)))
        except (TypeError, ValueError):
            normalized_top_n = "all"

    anomalies_only_flag = str(anomalies_only).lower() in {"true", "1", "yes", "on"}
    if anomalies_only_flag and not block01_df.empty:
        series = block01_df[metric_sort_column].astype(float)
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr > 0:
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            block01_df = block01_df[(series < lower_bound) | (series > upper_bound)]

    if block01_df.empty:
        return (), normalized_sort_order, requested_metric, normalized_top_n, anomalies_only_flag

    if normalized_sort_order == "highest":
        block01_df = block01_df.sort_values(
            [metric_sort_column, "app_description", "product"],
            ascending=[False, True, True],
            kind="mergesort"
        )
    elif normalized_sort_order == "least":
        block01_df = block01_df.sort_values(
            [metric_sort_column, "app_description", "product"],
            ascending=[True, True, True],
            kind="mergesort"
        )

    if normalized_top_n != "all":
        block01_df = block01_df.head(int(normalized_top_n))

    block01_df = block01_df.reset_index(drop=True)
    block01_df["rank"] = block01_df.index + 1
    rows = tuple(block01_df.to_dict(orient="records"))
    return rows, normalized_sort_order, requested_metric, normalized_top_n, anomalies_only_flag


def analyze_data(filepath, days_window="all", product_filter="all", company_filter="all"):
    return get_analyzer_module().analyze_data(filepath, days_window, product_filter, company_filter)


def analyze_merchant(df, merchant_name):
    return get_analyzer_module().analyze_merchant(df, merchant_name)


def analyze_dataset_comparison(
    filepath_a,
    filepath_b,
    days_window="all",
    product_filter="all",
    company_filter="all",
    compare_columns=None,
    compare_category="Product",
    compare_items=None,
):
    return get_analyzer_module().analyze_dataset_comparison(
        filepath_a,
        filepath_b,
        days_window,
        product_filter,
        company_filter,
        compare_columns,
        compare_category,
        compare_items,
    )


def get_comparison_item_sets(
    filepath_a,
    filepath_b,
    days_window="all",
    product_filter="all",
    company_filter="all",
    compare_category="Product",
):
    return get_analyzer_module().get_comparison_item_sets(
        filepath_a,
        filepath_b,
        days_window,
        product_filter,
        company_filter,
        compare_category,
    )


def load_comparison_options(filepath_a, filepath_b):
    if not filepath_a or not filepath_b:
        return ["Count", "ValueNett"], ["Product", "CompanyName"]

    if not os.path.exists(filepath_a) or not os.path.exists(filepath_b):
        return ["Count", "ValueNett"], ["Product", "CompanyName"]

    available_compare_columns, available_categories = _load_comparison_options_cached(
        *_file_cache_key(filepath_a),
        *_file_cache_key(filepath_b),
    )
    return list(available_compare_columns), list(available_categories)


def save_uploaded_file(file_storage, upload_slot):
    filename = secure_filename(file_storage.filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    stored_name = f"{upload_slot}_{timestamp}_{filename}"
    filepath = os.path.join(UPLOAD_FOLDER, stored_name)
    file_storage.save(filepath)
    return filepath


def load_filter_options(filepath):
    if not filepath or not os.path.exists(filepath):
        return [], []

    product_options, company_options = _load_filter_options_cached(*_file_cache_key(filepath))
    return list(product_options), list(company_options)


def build_block01_rows(filepath, sort_order="all", ranking_metric="Count", top_n="all", anomalies_only=False):
    if not filepath or not os.path.exists(filepath):
        return [], "all", "Count", "all", False

    rows, normalized_sort_order, requested_metric, normalized_top_n, anomalies_only_flag = _build_block01_rows_cached(
        *_file_cache_key(filepath),
        sort_order,
        ranking_metric,
        top_n,
        anomalies_only,
    )
    rows = [dict(row) for row in rows]
    return rows, normalized_sort_order, requested_metric, normalized_top_n, anomalies_only_flag


def build_featured_chart(charts, comparison_charts):
    if comparison_charts:
        if comparison_charts.get("compare_selected_columns_chart"):
            return {
                "key": "compare_selected_columns_chart",
                "title": "Where is the strongest file-to-file variance?",
                "subtitle": "This chart compares the selected comparison metrics across File A and File B.",
                "html": comparison_charts["compare_selected_columns_chart"]
            }
        if comparison_charts.get("compare_selected_category_chart"):
            return {
                "key": "compare_selected_category_chart",
                "title": "Which shared category is shifting the most?",
                "subtitle": "This chart highlights the selected shared categories across both datasets.",
                "html": comparison_charts["compare_selected_category_chart"]
            }

    if charts:
        if charts.get("trend_chart"):
            return {
                "key": "trend_chart",
                "title": "How is value moving over time?",
                "subtitle": "This is the lead trend view for the currently filtered base dataset.",
                "html": charts["trend_chart"]
            }
        if charts.get("product_chart"):
            return {
                "key": "product_chart",
                "title": "Which products are carrying volume?",
                "subtitle": "This chart surfaces the highest-volume products in the active filtered dataset.",
                "html": charts["product_chart"]
            }

    return None


def build_chart_sections(charts, comparison_charts, featured_chart=None):
    featured_key = featured_chart.get("key") if featured_chart else None

    base_chart_meta = [
        ("trend_chart", "How is debit trending over time?", "Time-series view of debit totals in the filtered base dataset."),
        ("product_chart", "Which products drive transaction count?", "Top product counts ranked in the current base dataset."),
        ("share_chart", "How is debit share distributed?", "Composition of debit value by product segment."),
        ("merchant_chart", "Which merchants drive debit value?", "Top merchants ranked by debit volume."),
        ("revenue_chart", "What does transaction scale look like?", "Distribution of transactions across value bands."),
    ]

    comparison_chart_meta = [
        ("compare_selected_columns_chart", "How do selected metrics compare?", "Grouped comparison of selected File A and File B totals."),
        ("compare_selected_category_chart", "Which shared categories shift most?", "Shared-category comparison across selected metrics."),
        ("compare_product_chart", "Which shared products differ most?", "Product-level comparison of debit across both files."),
        ("compare_company_chart", "Which shared merchants differ most?", "Merchant-level debit comparison for overlapping companies."),
        ("compare_trend_chart", "How do the two trends diverge over time?", "Time-series comparison of debit totals across both files."),
    ]

    def build_cards(source, meta):
        cards = []
        for key, title, subtitle in meta:
            if key == featured_key:
                continue
            if source and source.get(key):
                cards.append({
                    "key": key,
                    "title": title,
                    "subtitle": subtitle,
                    "html": source[key]
                })
        return cards

    return {
        "base": build_cards(charts, base_chart_meta),
        "comparison": build_cards(comparison_charts, comparison_chart_meta)
    }


def build_currency_rows(filtered_df):
    if filtered_df is None or filtered_df.empty:
        return []

    if "Currency" not in filtered_df.columns:
        return []

    currency_df = filtered_df.copy()
    currency_df["Currency"] = currency_df["Currency"].fillna("").astype(str).str.strip().str.upper()
    currency_df = currency_df[currency_df["Currency"].isin(ALLOWED_CURRENCY_CODES)]
    if currency_df.empty:
        return []

    for numeric_col in ["Count", "ValueNett", "ValueDebit", "ValueCredit"]:
        if numeric_col in currency_df.columns:
            currency_df[numeric_col] = pd.to_numeric(currency_df[numeric_col], errors="coerce").fillna(0)
        else:
            currency_df[numeric_col] = 0

    grouped = (
        currency_df.groupby("Currency", dropna=False)
        .agg(
            transactions=("Currency", "size"),
            total_count=("Count", "sum"),
            total_valuenett=("ValueNett", "sum"),
            total_valuedebit=("ValueDebit", "sum"),
            total_valuecredit=("ValueCredit", "sum"),
        )
        .reset_index()
    )

    currency_rank = {code: index for index, code in enumerate(CURRENCY_DISPLAY_ORDER)}
    grouped["_currency_rank"] = grouped["Currency"].map(currency_rank).fillna(999).astype(int)
    grouped = grouped.sort_values("_currency_rank", ascending=True).drop(columns=["_currency_rank"]) 

    return grouped.to_dict(orient="records")


def build_kpi_trends(filtered_df):
    default = {
        "transactions": {"delta_pct": None, "direction": "flat", "label": "No trend"},
        "debit": {"delta_pct": None, "direction": "flat", "label": "No trend"},
        "credit": {"delta_pct": None, "direction": "flat", "label": "No trend"},
    }

    if filtered_df is None or filtered_df.empty or "DateOfWeek" not in filtered_df.columns:
        return default

    trend_df = filtered_df.copy()
    trend_df["DateOfWeek"] = pd.to_datetime(trend_df["DateOfWeek"], errors="coerce")
    trend_df = trend_df.dropna(subset=["DateOfWeek"]).sort_values("DateOfWeek")

    if trend_df.empty:
        return default

    latest_date = trend_df["DateOfWeek"].max()
    period_days = 7
    current_start = latest_date - pd.Timedelta(days=period_days - 1)
    previous_start = current_start - pd.Timedelta(days=period_days)
    previous_end = current_start - pd.Timedelta(seconds=1)

    current_df = trend_df[trend_df["DateOfWeek"] >= current_start]
    previous_df = trend_df[(trend_df["DateOfWeek"] >= previous_start) & (trend_df["DateOfWeek"] <= previous_end)]

    def summarize_delta(current_value, previous_value):
        if previous_value == 0:
            if current_value == 0:
                return {"delta_pct": 0.0, "direction": "flat", "label": "No change"}
            return {"delta_pct": None, "direction": "up", "label": "New activity"}

        delta_pct = ((current_value - previous_value) / abs(previous_value)) * 100
        if delta_pct > 0.0001:
            direction = "up"
        elif delta_pct < -0.0001:
            direction = "down"
        else:
            direction = "flat"

        return {
            "delta_pct": delta_pct,
            "direction": direction,
            "label": f"{delta_pct:+.1f}% vs previous 7 days"
        }

    current_tx = len(current_df)
    previous_tx = len(previous_df)
    current_debit = pd.to_numeric(current_df.get("ValueDebit", 0), errors="coerce").fillna(0).sum()
    previous_debit = pd.to_numeric(previous_df.get("ValueDebit", 0), errors="coerce").fillna(0).sum()
    current_credit = pd.to_numeric(current_df.get("ValueCredit", 0), errors="coerce").fillna(0).sum()
    previous_credit = pd.to_numeric(previous_df.get("ValueCredit", 0), errors="coerce").fillna(0).sum()

    trends = {
        "transactions": summarize_delta(current_tx, previous_tx),
        "debit": summarize_delta(current_debit, previous_debit),
        "credit": summarize_delta(current_credit, previous_credit),
    }

    return trends


def build_kpi_sparklines(filtered_df):
    default = {
        "transactions": {"points": "0,16 120,16", "direction": "flat", "has_data": False},
        "debit": {"points": "0,16 120,16", "direction": "flat", "has_data": False},
        "credit": {"points": "0,16 120,16", "direction": "flat", "has_data": False},
    }

    if filtered_df is None or filtered_df.empty or "DateOfWeek" not in filtered_df.columns:
        return default

    spark_df = filtered_df.copy()
    spark_df["DateOfWeek"] = pd.to_datetime(spark_df["DateOfWeek"], errors="coerce")
    spark_df = spark_df.dropna(subset=["DateOfWeek"])
    if spark_df.empty:
        return default

    spark_df["DateKey"] = spark_df["DateOfWeek"].dt.floor("D")
    daily = (
        spark_df.groupby("DateKey", dropna=False)
        .agg(
            transactions=("DateKey", "size"),
            debit=("ValueDebit", "sum"),
            credit=("ValueCredit", "sum"),
        )
        .sort_index()
    )

    latest_day = daily.index.max()
    date_range = pd.date_range(end=latest_day, periods=14, freq="D")
    daily = daily.reindex(date_range, fill_value=0)

    def series_to_points(values, width=120, height=32):
        clean_values = [float(v) for v in values]
        count = len(clean_values)
        if count <= 1:
            return "0,16 120,16", "flat"

        min_value = min(clean_values)
        max_value = max(clean_values)
        span = max_value - min_value
        step_x = width / (count - 1)

        point_parts = []
        for index, value in enumerate(clean_values):
            x = index * step_x
            if span == 0:
                y = height / 2
            else:
                normalized = (value - min_value) / span
                y = height - (normalized * height)
            point_parts.append(f"{x:.2f},{y:.2f}")

        last_value = clean_values[-1]
        previous_value = clean_values[-2]
        if last_value > previous_value:
            direction = "up"
        elif last_value < previous_value:
            direction = "down"
        else:
            direction = "flat"

        return " ".join(point_parts), direction

    tx_points, tx_direction = series_to_points(daily["transactions"].tolist())
    debit_points, debit_direction = series_to_points(daily["debit"].tolist())
    credit_points, credit_direction = series_to_points(daily["credit"].tolist())

    return {
        "transactions": {"points": tx_points, "direction": tx_direction, "has_data": True},
        "debit": {"points": debit_points, "direction": debit_direction, "has_data": True},
        "credit": {"points": credit_points, "direction": credit_direction, "has_data": True},
    }


def build_storytelling_insights(filtered_df, results, comparison_results, kpi_trends):
    insights = {
        "what_changed": {
            "title": "What changed this week?",
            "bullets": []
        },
        "top_drivers": {
            "title": "What is driving the numbers?",
            "bullets": []
        },
        "comparison_story": {
            "title": "Where do datasets disagree most?",
            "bullets": []
        }
    }

    tx_trend = (kpi_trends or {}).get("transactions", {})
    debit_trend = (kpi_trends or {}).get("debit", {})
    credit_trend = (kpi_trends or {}).get("credit", {})
    insights["what_changed"]["bullets"].append({"text": f"Transactions: {tx_trend.get('label', 'No trend')}", "target": "#overview"})
    insights["what_changed"]["bullets"].append({"text": f"Debit movement: {debit_trend.get('label', 'No trend')}", "target": "#overview"})
    insights["what_changed"]["bullets"].append({"text": f"Credit movement: {credit_trend.get('label', 'No trend')}", "target": "#overview"})

    if results:
        top_products = results.get("top_products")
        if top_products is not None and len(top_products) > 0:
            top_product_name, top_product_count = next(iter(top_products.items()))
            insights["top_drivers"]["bullets"].append({
                "text": f"Top product by transaction volume: {top_product_name} ({fmt_num(top_product_count)} records).",
                "target": "#block01"
            })

        top_companies = results.get("top_companies")
        if top_companies is not None and len(top_companies) > 0:
            top_company_name, top_company_count = next(iter(top_companies.items()))
            insights["top_drivers"]["bullets"].append({
                "text": f"Top merchant by activity: {top_company_name} ({fmt_num(top_company_count)} transactions).",
                "target": "#compare-controls"
            })

    currency_rows = build_currency_rows(filtered_df)
    if currency_rows:
        lead_currency = currency_rows[0]
        insights["top_drivers"]["bullets"].append({
            "text": f"Lead currency: {lead_currency.get('Currency', 'N/A')} with {fmt_num(lead_currency.get('transactions', 0))} transactions.",
            "target": "#block02"
        })

    if not insights["top_drivers"]["bullets"]:
        insights["top_drivers"]["bullets"].append({
            "text": "Upload and analyze a dataset to identify product, merchant, and currency drivers.",
            "target": "#upload-controls"
        })

    if comparison_results:
        all_count = comparison_results.get("all_in_comparison_count", 0)
        shared_count = comparison_results.get("shared_in_both_count", 0)
        unique_count = comparison_results.get("unique_in_comparison_count", 0)
        insights["comparison_story"]["bullets"].append({
            "text": f"File B contains {fmt_num(all_count)} items, with {fmt_num(shared_count)} shared and {fmt_num(unique_count)} unique to comparison.",
            "target": "#block03"
        })

        column_rows = comparison_results.get("column_comparisons", [])
        if column_rows:
            most_divergent = max(column_rows, key=lambda row: abs(float(row.get("delta", 0))))
            delta_value = float(most_divergent.get("delta", 0))
            direction_text = "higher in File B" if delta_value > 0 else "lower in File B" if delta_value < 0 else "matched"
            insights["comparison_story"]["bullets"].append({
                "text": f"Largest metric gap: {most_divergent.get('column', 'N/A')} is {fmt_num(abs(delta_value))} {direction_text}.",
                "target": "#compare-controls"
            })

        selected_count = len(comparison_results.get("selected_compare_items", []))
        insights["comparison_story"]["bullets"].append({
            "text": f"Current comparison scope uses {fmt_num(selected_count)} selected items for metric calculation.",
            "target": "#compare-controls"
        })
    else:
        insights["comparison_story"]["bullets"].append({
            "text": "Upload File B and apply comparison settings to reveal mismatch insights.",
            "target": "#upload-controls"
        })

    return insights


def answer_storytelling_question(question_text, filtered_df, results, comparison_results, kpi_trends):
    question = str(question_text or "").strip()
    if not question:
        return "Ask a question about trends, top drivers, currency concentration, or comparison mismatch."

    lower_q = question.lower()

    if any(token in lower_q for token in ["trend", "week", "change", "up", "down"]):
        tx_label = (kpi_trends or {}).get("transactions", {}).get("label", "No trend")
        debit_label = (kpi_trends or {}).get("debit", {}).get("label", "No trend")
        credit_label = (kpi_trends or {}).get("credit", {}).get("label", "No trend")
        return f"Recent trend summary: Transactions {tx_label}; Debit {debit_label}; Credit {credit_label}."

    if any(token in lower_q for token in ["top product", "product", "driver", "driving"]):
        if results and results.get("top_products"):
            top_product_name, top_product_count = next(iter(results["top_products"].items()))
            return f"Top product by activity is {top_product_name} with {fmt_num(top_product_count)} records in the active filter set."
        return "No product driver is available yet. Upload and analyze a base dataset first."

    if any(token in lower_q for token in ["merchant", "company", "top company"]):
        if results and results.get("top_companies"):
            top_company_name, top_company_count = next(iter(results["top_companies"].items()))
            return f"Top merchant is {top_company_name} with {fmt_num(top_company_count)} transactions in the current view."
        return "No merchant ranking is available yet."

    if any(token in lower_q for token in ["currency", "currencies", "fx"]):
        currency_rows = build_currency_rows(filtered_df)
        if currency_rows:
            lead_currency = currency_rows[0]
            lead_tx = float(lead_currency.get("transactions", 0) or 0)
            total_tx = sum(float(row.get("transactions", 0) or 0) for row in currency_rows)
            dominance = (lead_tx / total_tx * 100) if total_tx > 0 else 0
            return (
                f"Lead currency is {lead_currency.get('Currency', 'N/A')} with {fmt_num(lead_tx)} transactions "
                f"({dominance:.1f}% share of currency-tagged activity)."
            )
        return "Currency insight is unavailable because the current dataset has no usable Currency values."

    if any(token in lower_q for token in ["compare", "comparison", "mismatch", "difference", "overlap", "file b", "file a"]):
        if comparison_results:
            all_count = float(comparison_results.get("all_in_comparison_count", 0) or 0)
            shared_count = float(comparison_results.get("shared_in_both_count", 0) or 0)
            unique_count = float(comparison_results.get("unique_in_comparison_count", 0) or 0)
            mismatch_ratio = (unique_count / all_count * 100) if all_count > 0 else 0
            column_rows = comparison_results.get("column_comparisons", [])
            if column_rows:
                most_divergent = max(column_rows, key=lambda row: abs(float(row.get("delta", 0) or 0)))
                delta_value = float(most_divergent.get("delta", 0) or 0)
                gap_direction = "higher" if delta_value > 0 else "lower" if delta_value < 0 else "equal"
                return (
                    f"Comparison summary: {fmt_num(shared_count)} shared items, {fmt_num(unique_count)} unique to File B "
                    f"({mismatch_ratio:.1f}% mismatch). Largest metric gap is {most_divergent.get('column', 'N/A')} "
                    f"with File B {gap_direction} by {fmt_num(abs(delta_value))}."
                )
            return (
                f"Comparison summary: {fmt_num(shared_count)} shared items and {fmt_num(unique_count)} unique to File B "
                f"({mismatch_ratio:.1f}% mismatch)."
            )
        return "Comparison insight is not available yet. Upload File B and apply comparison settings first."

    insights = build_storytelling_insights(filtered_df, results, comparison_results, kpi_trends)
    fallback_bullets = []
    for section_key in ["what_changed", "top_drivers", "comparison_story"]:
        section = insights.get(section_key, {})
        bullets = section.get("bullets", [])
        if bullets:
            fallback_bullets.append(bullets[0].get("text", ""))

    fallback_text = " | ".join([text for text in fallback_bullets if text])
    if fallback_text:
        return f"I could not map that question exactly. Here is the current summary: {fallback_text}"

    return "I do not have enough analyzed data yet. Upload files and run analysis, then ask again."


def build_insight_signals(filtered_df, results, comparison_results, kpi_trends):
    signals = []

    tx_trend = (kpi_trends or {}).get("transactions", {})
    debit_trend = (kpi_trends or {}).get("debit", {})
    credit_trend = (kpi_trends or {}).get("credit", {})

    def trend_signal(metric_name, trend_info):
        direction = trend_info.get("direction", "flat")
        delta_pct = trend_info.get("delta_pct")
        label = trend_info.get("label", "No trend")

        if direction == "up" and delta_pct is not None and delta_pct >= 20:
            level = "opportunity"
            title = f"{metric_name} surged"
        elif direction == "down" and delta_pct is not None and delta_pct <= -15:
            level = "risk"
            title = f"{metric_name} dropped"
        else:
            level = "info"
            title = f"{metric_name} stable"

        return {
            "level": level,
            "title": title,
            "detail": label,
            "target": "#overview"
        }

    signals.append(trend_signal("Transactions", tx_trend))
    signals.append(trend_signal("Debit", debit_trend))
    signals.append(trend_signal("Credit", credit_trend))

    if comparison_results:
        unique_count = float(comparison_results.get("unique_in_comparison_count", 0) or 0)
        all_count = float(comparison_results.get("all_in_comparison_count", 0) or 0)
        unique_ratio = (unique_count / all_count * 100) if all_count > 0 else 0

        if unique_ratio >= 45:
            mismatch_level = "risk"
            mismatch_title = "High dataset mismatch"
        elif unique_ratio >= 25:
            mismatch_level = "watch"
            mismatch_title = "Moderate dataset mismatch"
        else:
            mismatch_level = "info"
            mismatch_title = "Low dataset mismatch"

        signals.append({
            "level": mismatch_level,
            "title": mismatch_title,
            "detail": f"{unique_ratio:.1f}% of File B items are unique to comparison.",
            "target": "#block03"
        })

        column_rows = comparison_results.get("column_comparisons", [])
        if column_rows:
            biggest_gap = max(column_rows, key=lambda row: abs(float(row.get("delta", 0) or 0)))
            gap_value = float(biggest_gap.get("delta", 0) or 0)
            signals.append({
                "level": "watch" if abs(gap_value) > 0 else "info",
                "title": f"Largest variance: {biggest_gap.get('column', 'N/A')}",
                "detail": f"Delta is {fmt_num(gap_value)} (File B minus File A).",
                "target": "#compare-controls"
            })

    currency_rows = build_currency_rows(filtered_df)
    if currency_rows and len(currency_rows) > 1:
        lead_currency = currency_rows[0]
        total_tx = sum(float(row.get("transactions", 0) or 0) for row in currency_rows)
        dominance = (float(lead_currency.get("transactions", 0) or 0) / total_tx * 100) if total_tx > 0 else 0
        signals.append({
            "level": "watch" if dominance >= 60 else "info",
            "title": f"Currency concentration: {lead_currency.get('Currency', 'N/A')}",
            "detail": f"Accounts for {dominance:.1f}% of transactions.",
            "target": "#block02"
        })

    if not signals:
        signals.append({
            "level": "info",
            "title": "No signals yet",
            "detail": "Upload and analyze data to generate story signals.",
            "target": "#upload-controls"
        })

    return signals[:6]


def build_detailed_report_text():
    report_sections = build_detailed_report_sections()
    lines = []

    for section in report_sections:
        lines.append(section["title"])
        lines.append("-" * len(section["title"]))
        lines.extend(section["rows"])
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def build_detailed_report_sections():
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    overview_rows = [
        f"Generated At: {generated_at}",
        f"Date Window: {selected_window_global}",
        f"Product Filter: {selected_product_global}",
        f"Company Filter: {selected_company_global}",
        f"File A (Base): {os.path.basename(latest_upload_path_global) if latest_upload_path_global else 'Not uploaded'}",
        f"File B (Comparison): {os.path.basename(secondary_upload_path_global) if secondary_upload_path_global else 'Not uploaded'}",
    ]

    base_rows = []

    if results_global:
        base_rows.extend([
            f"Total Transactions: {fmt_num(results_global.get('total_transactions', 0))}",
            f"Total Debit: {fmt_num(results_global.get('total_debit', 0))}",
            f"Total Credit: {fmt_num(results_global.get('total_credit', 0))}",
            "Top Products:",
        ])

        top_products = results_global.get("top_products")
        if top_products is not None and len(top_products) > 0:
            for index, (name, value) in enumerate(top_products.items(), start=1):
                base_rows.append(f"{index}. {name}: {fmt_num(value)}")
        else:
            base_rows.append("No product summary available.")

        base_rows.append("Top Companies:")
        top_companies = results_global.get("top_companies")
        if top_companies is not None and len(top_companies) > 0:
            for index, (name, value) in enumerate(top_companies.items(), start=1):
                base_rows.append(f"{index}. {name}: {fmt_num(value)}")
        else:
            base_rows.append("No company summary available.")
    else:
        base_rows.append("No analysis results available yet.")

    currency_section_rows = []
    currency_rows = build_currency_rows(df_global)
    if currency_rows:
        currency_section_rows.append("Currency | Transactions | Count | ValueNett | ValueDebit | ValueCredit")
        for row in currency_rows:
            currency_section_rows.append(
                f"{row.get('Currency', '')} | "
                f"{fmt_num(row.get('transactions', 0))} | "
                f"{fmt_num(row.get('total_count', 0))} | "
                f"{fmt_num(row.get('total_valuenett', 0))} | "
                f"{fmt_num(row.get('total_valuedebit', 0))} | "
                f"{fmt_num(row.get('total_valuecredit', 0))}"
            )
    else:
        currency_section_rows.append("No currency data available.")

    comparison_rows = []
    if comparison_results_global:
        comparison_rows.extend([
            f"Comparison Category: {comparison_results_global.get('selected_compare_category', 'N/A')}",
            f"Comparison Columns: {', '.join(comparison_results_global.get('selected_compare_columns', [])) or 'N/A'}",
            f"Selected Item Scope: {selected_compare_item_scope_global}",
            f"Items Selected For Comparison: {len(comparison_results_global.get('selected_compare_items', []))}",
            f"All Items In File B: {comparison_results_global.get('all_in_comparison_count', 0)}",
            f"Items Found In Both: {comparison_results_global.get('shared_in_both_count', 0)}",
            f"Items Only In File B: {comparison_results_global.get('unique_in_comparison_count', 0)}",
            "Column Totals (File A vs File B):",
        ])

        for row in comparison_results_global.get("column_comparisons", []):
            comparison_rows.append(
                f"{row.get('column', 'N/A')}: "
                f"A={fmt_num(row.get('file_a_total', 0))}, "
                f"B={fmt_num(row.get('file_b_total', 0))}, "
                f"Delta={fmt_num(row.get('delta', 0))}"
            )

        comparison_rows.append("Selected Items (Preview):")
        selected_items = comparison_results_global.get("selected_compare_items", [])
        if selected_items:
            for index, item in enumerate(selected_items[:50], start=1):
                comparison_rows.append(f"{index}. {item}")
        else:
            comparison_rows.append("No selected comparison items.")
    else:
        comparison_rows.append("No comparison results available yet.")

    return [
        {"title": "Exan Detailed Analysis Report", "rows": overview_rows},
        {"title": "Base Dataset Summary", "rows": base_rows},
        {"title": "Currency Breakdown", "rows": currency_section_rows},
        {"title": "Comparison Summary", "rows": comparison_rows},
    ]


def build_detailed_report_csv_content():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["section", "detail"])

    for section in build_detailed_report_sections():
        for row in section["rows"]:
            writer.writerow([section["title"], row])

    return output.getvalue()


def build_detailed_report_pdf_content():
    reportlab_canvas = importlib.import_module("reportlab.pdfgen.canvas")
    reportlab_pagesizes = importlib.import_module("reportlab.lib.pagesizes")

    buffer = io.BytesIO()
    pdf = reportlab_canvas.Canvas(buffer, pagesize=reportlab_pagesizes.A4)
    page_width, page_height = reportlab_pagesizes.A4
    left_margin = 40
    top_margin = 40
    line_height = 14
    y = page_height - top_margin

    def draw_line(text, font_name="Helvetica", font_size=10):
        nonlocal y
        if y <= top_margin:
            pdf.showPage()
            y = page_height - top_margin
        pdf.setFont(font_name, font_size)
        safe_text = str(text)[:160]
        pdf.drawString(left_margin, y, safe_text)
        y -= line_height

    for section in build_detailed_report_sections():
        draw_line(section["title"], "Helvetica-Bold", 12)
        for row in section["rows"]:
            draw_line(row)
        draw_line("")

    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def _slug_token(raw_value, fallback="all"):
    value = str(raw_value or "").strip().lower()
    if not value:
        value = fallback
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return (value or fallback)[:32]


def build_report_filename(extension):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix_parts = [
        f"w-{_slug_token(selected_window_global, 'all')}",
        f"p-{_slug_token(selected_product_global, 'all')}",
        f"c-{_slug_token(selected_company_global, 'all')}",
    ]

    if comparison_results_global:
        suffix_parts.extend([
            f"cat-{_slug_token(selected_compare_category_global, 'none')}",
            f"scope-{_slug_token(selected_compare_item_scope_global, 'both')}",
        ])

    suffix = "_".join(suffix_parts)
    return f"exan_detailed_report_{suffix}_{timestamp}.{extension}"


def build_report_meta():
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date_window": selected_window_global,
        "product_filter": selected_product_global,
        "company_filter": selected_company_global,
        "primary_filename": os.path.basename(latest_upload_path_global) if latest_upload_path_global and os.path.exists(latest_upload_path_global) else "Not uploaded",
        "secondary_filename": os.path.basename(secondary_upload_path_global) if secondary_upload_path_global and os.path.exists(secondary_upload_path_global) else "Not uploaded",
    }


def build_comparison_delta_rows(comparison_results):
    delta_rows = []

    for row in (comparison_results or {}).get("column_comparisons", []):
        file_a_total = float(row.get("file_a_total", 0) or 0)
        file_b_total = float(row.get("file_b_total", 0) or 0)
        delta_value = float(row.get("delta", file_b_total - file_a_total) or 0)

        if delta_value > 0.0001:
            tone = "positive"
            status_label = "Higher in File B"
        elif delta_value < -0.0001:
            tone = "negative"
            status_label = "Lower in File B"
        else:
            tone = "neutral"
            status_label = "Matched"

        if abs(file_a_total) > 0:
            delta_pct = (delta_value / abs(file_a_total)) * 100
            delta_pct_label = f"{delta_pct:+.1f}%"
        elif file_b_total != 0:
            delta_pct = None
            delta_pct_label = "New in File B"
        else:
            delta_pct = 0.0
            delta_pct_label = "0.0%"

        delta_rows.append({
            "column": row.get("column", "N/A"),
            "file_a_total": file_a_total,
            "file_b_total": file_b_total,
            "delta": delta_value,
            "delta_pct": delta_pct,
            "delta_pct_label": delta_pct_label,
            "tone": tone,
            "status_label": status_label,
        })

    return delta_rows


def build_comparison_health(comparison_results):
    default_state = {
        "tone": "neutral",
        "label": "No comparison loaded",
        "mismatch_ratio": 0.0,
        "shared_ratio": 0.0,
    }

    if not comparison_results:
        return default_state

    all_count = float(comparison_results.get("all_in_comparison_count", 0) or 0)
    shared_count = float(comparison_results.get("shared_in_both_count", 0) or 0)
    unique_count = float(comparison_results.get("unique_in_comparison_count", 0) or 0)

    if all_count <= 0:
        return {
            "tone": "neutral",
            "label": "Comparison dataset is empty",
            "mismatch_ratio": 0.0,
            "shared_ratio": 0.0,
        }

    mismatch_ratio = (unique_count / all_count) * 100
    shared_ratio = (shared_count / all_count) * 100

    if mismatch_ratio >= 45:
        tone = "negative"
        label = f"High mismatch: {mismatch_ratio:.1f}% unique to File B"
    elif mismatch_ratio >= 25:
        tone = "watch"
        label = f"Moderate mismatch: {mismatch_ratio:.1f}% unique to File B"
    else:
        tone = "positive"
        label = f"Low mismatch: {mismatch_ratio:.1f}% unique to File B"

    return {
        "tone": tone,
        "label": label,
        "mismatch_ratio": mismatch_ratio,
        "shared_ratio": shared_ratio,
    }


def build_report_preview_context():
    comparison_results = comparison_results_global
    comparison_charts = comparison_charts_global
    report_meta = build_report_meta()
    kpi_trends = build_kpi_trends(df_global)
    featured_chart = build_featured_chart(charts_global, comparison_charts)

    return {
        "report_meta": report_meta,
        "results": results_global,
        "comparison_results": comparison_results,
        "comparison_delta_rows": build_comparison_delta_rows(comparison_results),
        "comparison_health": build_comparison_health(comparison_results),
        "currency_rows": build_currency_rows(df_global),
        "storytelling_insights": build_storytelling_insights(df_global, results_global, comparison_results, kpi_trends),
        "insight_signals": build_insight_signals(df_global, results_global, comparison_results, kpi_trends),
        "featured_chart": featured_chart,
        "chart_sections": build_chart_sections(charts_global, comparison_charts, featured_chart),
        "report_sections": build_detailed_report_sections(),
    }


def persist_auth_settings(username, password, env_path=".env"):
    settings = {
        "APP_USERNAME": username,
        "APP_PASSWORD": password
    }

    existing_lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as env_file:
            existing_lines = env_file.readlines()

    updated_keys = set()
    output_lines = []

    for raw_line in existing_lines:
        stripped = raw_line.strip()

        if not stripped or stripped.startswith("#") or "=" not in stripped:
            output_lines.append(raw_line)
            continue

        key = stripped.split("=", 1)[0].strip()

        if key in settings:
            output_lines.append(f"{key}={settings[key]}\n")
            updated_keys.add(key)
        else:
            output_lines.append(raw_line)

    for key, value in settings.items():
        if key not in updated_keys:
            output_lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as env_file:
        env_file.writelines(output_lines)


def resolve_react_login_url(req):
    if REACT_LOGIN_URL:
        return REACT_LOGIN_URL
    return None


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok", "service": "exan-dashboard"}), 200


@app.route("/login", methods=["GET", "POST"])
def login():
    react_login_url = resolve_react_login_url(request)

    if request.method == "GET":
        if react_login_url:
            return redirect(react_login_url)
        return render_template("login.html", error=None)

    if request.method == "POST":

        if request.is_json:
            data = request.get_json(silent=True) or {}
            username = data.get("username", "")
            password = data.get("password", "")
            remember = bool(data.get("remember", False))

            if username == APP_USERNAME and password == APP_PASSWORD:
                session["authenticated"] = True
                session.permanent = remember
                return jsonify({
                    "success": True,
                    "message": "Login successful",
                    "redirect_url": url_for("index")
                })

            return jsonify({"success": False, "message": "Invalid username or password"}), 401

        username = request.form.get("username", "")
        password = request.form.get("password", "")
        remember = request.form.get("remember") == "on"

        if username == APP_USERNAME and password == APP_PASSWORD:
            session["authenticated"] = True
            session.permanent = remember
            return redirect(url_for("index"))

        if react_login_url:
            return redirect(url_for("login"))
        return render_template("login.html", error="Invalid username or password")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/settings", methods=["GET", "POST"])
def settings():
    if not session.get("authenticated"):
        return redirect(url_for("login"))

    global APP_USERNAME
    global APP_PASSWORD

    error = None
    success = None

    if request.method == "POST":
        current_username = request.form.get("current_username", "").strip()
        current_password = request.form.get("current_password", "")
        new_username = request.form.get("new_username", "").strip()
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        if current_username != APP_USERNAME:
            error = "Current username is incorrect."
        elif current_password != APP_PASSWORD:
            error = "Current password is incorrect."
        elif not new_username:
            error = "Username cannot be empty."
        elif len(new_password) < 6:
            error = "New password must be at least 6 characters long."
        elif new_password != confirm_password:
            error = "Password confirmation does not match."
        else:
            APP_USERNAME = new_username
            APP_PASSWORD = new_password
            os.environ["APP_USERNAME"] = new_username
            os.environ["APP_PASSWORD"] = new_password
            persist_auth_settings(new_username, new_password)
            success = "Settings updated successfully. Use the new credentials on next login."

    return render_template("settings.html", current_username=APP_USERNAME, error=error, success=success)


@app.route("/advanced/charts", methods=["GET"])
def advanced_charts():
    if not session.get("authenticated"):
        return redirect(url_for("login"))

    featured_chart = build_featured_chart(charts_global, comparison_charts_global)
    chart_sections = build_chart_sections(charts_global, comparison_charts_global, featured_chart)

    return render_template(
        "advanced/charts.html",
        featured_chart=featured_chart,
        chart_sections=chart_sections,
        has_primary_upload=bool(latest_upload_path_global and os.path.exists(latest_upload_path_global)),
        has_secondary_upload=bool(secondary_upload_path_global and os.path.exists(secondary_upload_path_global)),
        primary_filename=os.path.basename(latest_upload_path_global) if latest_upload_path_global and os.path.exists(latest_upload_path_global) else None,
        secondary_filename=os.path.basename(secondary_upload_path_global) if secondary_upload_path_global and os.path.exists(secondary_upload_path_global) else None,
    )


@app.route("/advanced/chart-fragment/<chart_key>", methods=["GET"])
def advanced_chart_fragment(chart_key):
    if not session.get("authenticated"):
        return jsonify({"error": "Unauthorized"}), 401

    normalized_key = str(chart_key or "").strip()
    if not re.fullmatch(r"[a-zA-Z0-9_\-]+", normalized_key):
        return jsonify({"error": "Invalid chart key"}), 400

    chart_html = None
    if charts_global and normalized_key in charts_global:
        chart_html = charts_global.get(normalized_key)
    elif comparison_charts_global and normalized_key in comparison_charts_global:
        chart_html = comparison_charts_global.get(normalized_key)

    if not chart_html:
        return jsonify({"error": "Chart not found"}), 404

    return jsonify({"chart_key": normalized_key, "html": chart_html})


@app.route("/", methods=["GET", "POST"])
def index():

    if not session.get("authenticated"):
        return redirect(url_for("login"))

    global df_global
    global charts_global
    global results_global
    global selected_window_global
    global selected_product_global
    global selected_company_global
    global latest_upload_path_global
    global secondary_upload_path_global
    global comparison_results_global
    global comparison_charts_global
    global selected_compare_columns_global
    global selected_compare_category_global
    global selected_compare_items_global
    global selected_compare_item_scope_global
    global storytelling_qa_history_global
    global auto_corrections_primary_global
    global auto_corrections_secondary_global

    results = results_global
    merchants = df_global["CompanyName"].dropna().unique() if df_global is not None else None
    merchant_results = None
    selected_window = selected_window_global
    selected_product = selected_product_global
    selected_company = selected_company_global
    comparison_results = comparison_results_global
    comparison_charts = comparison_charts_global
    selected_compare_columns = selected_compare_columns_global
    selected_compare_category = selected_compare_category_global
    available_compare_columns = ["Count", "ValueNett"]
    available_compare_categories = ["Product", "CompanyName"]
    available_compare_item_scopes = ["both", "all_in_comparison", "only_in_comparison"]
    available_compare_items = []
    selected_compare_items = selected_compare_items_global
    selected_compare_item_scope = selected_compare_item_scope_global
    upload_error = None
    block01_sort_order = str(request.args.get("block01_sort", "all") or "all").strip().lower()
    block01_metric = str(request.args.get("block01_metric", "Count") or "Count").strip()
    block01_top_n = str(request.args.get("block01_top_n", "all") or "all").strip()
    block01_anomalies_only = str(request.args.get("block01_anomalies_only", "") or "").strip().lower() in {"1", "true", "on", "yes"}
    block01_rows = []
    page_notice = None
    currency_rows = build_currency_rows(df_global)
    kpi_trends = build_kpi_trends(df_global)
    kpi_sparklines = build_kpi_sparklines(df_global)
    storytelling_insights = build_storytelling_insights(df_global, results, comparison_results, kpi_trends)
    insight_signals = build_insight_signals(df_global, results, comparison_results, kpi_trends)
    featured_chart = build_featured_chart(charts_global, comparison_charts)
    chart_sections = build_chart_sections(charts_global, comparison_charts, featured_chart)
    comparison_delta_rows = build_comparison_delta_rows(comparison_results)
    comparison_health = build_comparison_health(comparison_results)
    report_meta = build_report_meta()
    storytelling_answer = None
    storytelling_question = ""
    storytelling_qa_history = storytelling_qa_history_global[-6:]
    auto_corrections_primary = int(auto_corrections_primary_global or 0)
    auto_corrections_secondary = int(auto_corrections_secondary_global or 0)
    comparison_ready = bool(
        latest_upload_path_global
        and secondary_upload_path_global
        and os.path.exists(latest_upload_path_global)
        and os.path.exists(secondary_upload_path_global)
    )

    product_options = None
    company_options = None

    if latest_upload_path_global and os.path.exists(latest_upload_path_global):
        try:
            product_options, company_options = load_filter_options(latest_upload_path_global)
            block01_rows, block01_sort_order, block01_metric, block01_top_n, block01_anomalies_only = build_block01_rows(
                latest_upload_path_global,
                block01_sort_order,
                block01_metric,
                block01_top_n,
                block01_anomalies_only
            )
        except ValueError as exc:
            upload_error = str(exc)
            product_options, company_options = [], []
            block01_rows = []

    if comparison_ready:
        try:
            available_compare_columns, available_compare_categories = load_comparison_options(
                latest_upload_path_global,
                secondary_upload_path_global
            )
            selected_compare_columns = [
                col for col in selected_compare_columns_global if col in available_compare_columns
            ] or available_compare_columns[:1]
            if selected_compare_category_global in available_compare_categories:
                selected_compare_category = selected_compare_category_global
            elif available_compare_categories:
                selected_compare_category = available_compare_categories[0]

            item_sets = get_comparison_item_sets(
                latest_upload_path_global,
                secondary_upload_path_global,
                selected_window,
                selected_product,
                selected_company,
                selected_compare_category,
            )
            scope_to_items = {
                "both": item_sets.get("shared_in_both", []),
                "all_in_comparison": item_sets.get("all_in_comparison", []),
                "only_in_comparison": item_sets.get("unique_in_comparison", []),
            }
            if selected_compare_item_scope not in scope_to_items:
                selected_compare_item_scope = "both"
            available_compare_items = scope_to_items[selected_compare_item_scope]
            selected_compare_items = [
                item for item in selected_compare_items_global if item in available_compare_items
            ]
            if not selected_compare_items:
                selected_compare_items = available_compare_items[:]
        except ValueError as exc:
            upload_error = str(exc)

    if request.method == "POST":
        if request.form.get("storytelling_ask") == "1":
            storytelling_question = str(request.form.get("storytelling_question", "") or "").strip()
            storytelling_answer = answer_storytelling_question(
                storytelling_question,
                df_global,
                results,
                comparison_results,
                kpi_trends,
            )
            if storytelling_question:
                storytelling_qa_history_global.append({
                    "question": storytelling_question,
                    "answer": storytelling_answer,
                    "asked_at": datetime.now().strftime("%H:%M"),
                })
                storytelling_qa_history_global = storytelling_qa_history_global[-12:]
            storytelling_qa_history = storytelling_qa_history_global[-6:]
            page_notice = "Storytelling answer generated."
        else:
            selected_window = request.form.get("date_window", selected_window_global)
            selected_product = request.form.get("product_filter", selected_product_global)
            selected_company = request.form.get("company_filter", selected_company_global)
            selected_compare_columns = request.form.getlist("compare_columns")
            selected_compare_category = request.form.get("compare_category", selected_compare_category_global)
            selected_compare_items = request.form.getlist("compare_items")
            selected_compare_item_scope = request.form.get("compare_item_scope", selected_compare_item_scope_global)

            selected_window_global = selected_window
            selected_product_global = selected_product
            selected_company_global = selected_company

            has_new_upload = False

        # Primary file upload (File A)
            primary_file = request.files.get("file_primary") or request.files.get("file")
            has_new_primary_upload = bool(primary_file and primary_file.filename != "")
            if has_new_primary_upload:
                latest_upload_path_global = save_uploaded_file(primary_file, "primary")
                has_new_upload = True

        # Secondary file upload (File B)
            secondary_file = request.files.get("file_secondary")
            has_new_secondary_upload = bool(secondary_file and secondary_file.filename != "")
            if has_new_secondary_upload:
                secondary_upload_path_global = save_uploaded_file(secondary_file, "secondary")
                has_new_upload = True

            if has_new_primary_upload and not has_new_secondary_upload:
                secondary_upload_path_global = None
                comparison_results_global = None
                comparison_charts_global = None
                selected_compare_items_global = []
                auto_corrections_secondary_global = 0
                auto_corrections_secondary = 0

            comparison_ready = bool(
                latest_upload_path_global
                and secondary_upload_path_global
                and os.path.exists(latest_upload_path_global)
                and os.path.exists(secondary_upload_path_global)
            )

            if comparison_ready:
                try:
                    available_compare_columns, available_compare_categories = load_comparison_options(
                        latest_upload_path_global,
                        secondary_upload_path_global
                    )
                except ValueError as exc:
                    upload_error = str(exc)
                    available_compare_columns, available_compare_categories = [], []

                selected_compare_columns = [
                    col for col in selected_compare_columns if col in available_compare_columns
                ]
                if not selected_compare_columns and available_compare_columns:
                    selected_compare_columns = available_compare_columns[:1]

                if selected_compare_category not in available_compare_categories and available_compare_categories:
                    selected_compare_category = available_compare_categories[0]

                item_sets = get_comparison_item_sets(
                    latest_upload_path_global,
                    secondary_upload_path_global,
                    selected_window,
                    selected_product,
                    selected_company,
                    selected_compare_category,
                )
                scope_to_items = {
                    "both": item_sets.get("shared_in_both", []),
                    "all_in_comparison": item_sets.get("all_in_comparison", []),
                    "only_in_comparison": item_sets.get("unique_in_comparison", []),
                }
                if selected_compare_item_scope not in scope_to_items:
                    selected_compare_item_scope = "both"
                available_compare_items = scope_to_items[selected_compare_item_scope]
                selected_compare_items = [item for item in selected_compare_items if item in available_compare_items]
                if not selected_compare_items:
                    selected_compare_items = available_compare_items[:]

                selected_compare_columns_global = selected_compare_columns
                selected_compare_category_global = selected_compare_category
                selected_compare_items_global = selected_compare_items
                selected_compare_item_scope_global = selected_compare_item_scope
            else:
                comparison_results_global = None
                comparison_charts_global = None
                comparison_results = None
                comparison_charts = None
                available_compare_items = []
                selected_compare_items = []

            if latest_upload_path_global and (has_new_upload or "apply_filters" in request.form):
                try:
                    # Analyze spreadsheet within selected filters.
                    df_global, results_global, charts_global = analyze_data(
                        latest_upload_path_global,
                        selected_window,
                        selected_product,
                        selected_company
                    )
                    results = results_global
                    auto_corrections_primary = int(df_global.attrs.get("auto_corrections_applied", 0) or 0)
                    auto_corrections_primary_global = auto_corrections_primary

                    merchants = df_global["CompanyName"].dropna().unique() if "CompanyName" in df_global.columns else []

                    if comparison_ready:
                        comparison_results_global, comparison_charts_global = analyze_dataset_comparison(
                            latest_upload_path_global,
                            secondary_upload_path_global,
                            selected_window,
                            selected_product,
                            selected_company,
                            selected_compare_columns,
                            selected_compare_category,
                            selected_compare_items
                        )
                        comparison_results = comparison_results_global
                        comparison_charts = comparison_charts_global
                        auto_corrections_secondary = int(comparison_results_global.get("auto_corrections_file_b", 0) or 0)
                        auto_corrections_secondary_global = auto_corrections_secondary
                    else:
                        comparison_results_global = None
                        comparison_charts_global = None
                        comparison_results = None
                        comparison_charts = None
                        auto_corrections_secondary_global = 0
                        auto_corrections_secondary = 0

                    currency_rows = build_currency_rows(df_global)
                    kpi_trends = build_kpi_trends(df_global)
                    kpi_sparklines = build_kpi_sparklines(df_global)
                    storytelling_insights = build_storytelling_insights(df_global, results, comparison_results, kpi_trends)
                    insight_signals = build_insight_signals(df_global, results, comparison_results, kpi_trends)
                    featured_chart = build_featured_chart(charts_global, comparison_charts)
                    chart_sections = build_chart_sections(charts_global, comparison_charts, featured_chart)
                    comparison_delta_rows = build_comparison_delta_rows(comparison_results)
                    comparison_health = build_comparison_health(comparison_results)
                    report_meta = build_report_meta()
                    page_notice = "Analysis updated successfully."
                except ValueError as exc:
                    upload_error = str(exc)

            if latest_upload_path_global and os.path.exists(latest_upload_path_global):
                try:
                    product_options, company_options = load_filter_options(latest_upload_path_global)
                    block01_rows, block01_sort_order, block01_metric, block01_top_n, block01_anomalies_only = build_block01_rows(
                        latest_upload_path_global,
                        block01_sort_order,
                        block01_metric,
                        block01_top_n,
                        block01_anomalies_only
                    )
                except ValueError as exc:
                    upload_error = str(exc)
                    product_options, company_options = [], []
                    block01_rows = []

            # Merchant analysis
            if "merchant" in request.form and df_global is not None:

                merchant = request.form["merchant"]
                merchant_results = analyze_merchant(df_global, merchant)

                merchants = df_global["CompanyName"].dropna().unique()

    return render_template(
        "index.html",
        results=results,
        merchants=merchants,
        merchant_results=merchant_results,
        charts=charts_global,
        selected_window=selected_window,
        selected_product=selected_product,
        selected_company=selected_company,
        product_options=product_options,
        company_options=company_options,
        comparison_results=comparison_results,
        comparison_charts=comparison_charts,
        available_compare_columns=available_compare_columns,
        selected_compare_columns=selected_compare_columns,
        available_compare_categories=available_compare_categories,
        selected_compare_category=selected_compare_category,
        available_compare_item_scopes=available_compare_item_scopes,
        selected_compare_item_scope=selected_compare_item_scope,
        available_compare_items=available_compare_items,
        selected_compare_items=selected_compare_items,
        page_notice=page_notice,
        report_meta=report_meta,
        comparison_delta_rows=comparison_delta_rows,
        comparison_health=comparison_health,
        storytelling_answer=storytelling_answer,
        storytelling_question=storytelling_question,
        storytelling_qa_history=storytelling_qa_history,
        upload_error=upload_error,
        has_primary_upload=bool(latest_upload_path_global and os.path.exists(latest_upload_path_global)),
        has_secondary_upload=bool(secondary_upload_path_global and os.path.exists(secondary_upload_path_global)),
        auto_corrections_primary=auto_corrections_primary,
        auto_corrections_secondary=auto_corrections_secondary,
        comparison_ready=comparison_ready,
        primary_filename=os.path.basename(latest_upload_path_global) if latest_upload_path_global and os.path.exists(latest_upload_path_global) else None,
        secondary_filename=os.path.basename(secondary_upload_path_global) if secondary_upload_path_global and os.path.exists(secondary_upload_path_global) else None,
        block01_rows=block01_rows,
        featured_chart=featured_chart,
        currency_rows=currency_rows,
        kpi_trends=kpi_trends,
        kpi_sparklines=kpi_sparklines,
        storytelling_insights=storytelling_insights,
        insight_signals=insight_signals,
        chart_sections=chart_sections,
        block01_sort_order=block01_sort_order,
        block01_metric=block01_metric,
        block01_top_n=block01_top_n,
        block01_anomalies_only=block01_anomalies_only,
        block01_metric_options=BLOCK01_METRIC_OPTIONS
    )


@app.route("/report-preview", methods=["GET"])
def report_preview():
    if not session.get("authenticated"):
        return redirect(url_for("login"))

    return render_template("report_preview.html", **build_report_preview_context())


@app.route("/export-detailed-report", methods=["GET"])
def export_detailed_report():
    if not session.get("authenticated"):
        return redirect(url_for("login"))

    report_format = str(request.args.get("format", "txt") or "txt").strip().lower()

    if report_format == "csv":
        payload = build_detailed_report_csv_content()
        content_type = "text/csv; charset=utf-8"
        extension = "csv"
    elif report_format == "pdf":
        payload = build_detailed_report_pdf_content()
        content_type = "application/pdf"
        extension = "pdf"
    else:
        payload = build_detailed_report_text()
        content_type = "text/plain; charset=utf-8"
        extension = "txt"

    filename = build_report_filename(extension)

    response = make_response(payload)
    response.headers["Content-Type"] = content_type
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@app.route("/export-detailed-report-csv", methods=["GET"])
def export_detailed_report_csv():
    if not session.get("authenticated"):
        return redirect(url_for("login"))
    return redirect(url_for("export_detailed_report", format="csv"))


@app.route("/export-detailed-report-pdf", methods=["GET"])
def export_detailed_report_pdf():
    if not session.get("authenticated"):
        return redirect(url_for("login"))
    return redirect(url_for("export_detailed_report", format="pdf"))


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=os.environ.get("FLASK_DEBUG", "0") == "1"
    )