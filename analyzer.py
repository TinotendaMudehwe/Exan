import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import html
import re


BASE_COLOR = "#3cc6ff"
COMPARE_COLOR = "#2dd37f"
DELTA_COLOR = "#ffb347"
ALERT_COLOR = "#ff7b7b"
GRID_COLOR = "rgba(190,210,255,0.14)"
ALLOWED_CURRENCY_CODES = {"USD", "ZWG", "ZIG", "ZWL"}


def _chart_html(fig):
    payload = fig.to_json()
    escaped_payload = html.escape(payload, quote=True)
    return (
        f'<div class="plotly-lazy" data-plotly="{escaped_payload}">'
        '<div class="plotly-lazy-placeholder">Loading chart...</div>'
        '</div>'
    )


def _canonicalize_columns(df):
    alias_map = {
        "dateofweek": "DateOfWeek",
        "dateof week": "DateOfWeek",
        "date of week": "DateOfWeek",
        "date": "DateOfWeek",
        "product": "Product",
        "companyname": "CompanyName",
        "company name": "CompanyName",
        "company": "CompanyName",
        "merchant": "CompanyName",
        "merchant name": "CompanyName",
        "currency": "Currency",
        "ccy": "Currency",
        "count": "Count",
        "valuenett": "ValueNett",
        "value nett": "ValueNett",
        "nett": "ValueNett",
        "valuedebit": "ValueDebit",
        "value debit": "ValueDebit",
        "valuecredit": "ValueCredit",
        "value credit": "ValueCredit",
    }

    rename_map = {}
    for column in df.columns:
        normalized = re.sub(r"\s+", " ", str(column).strip().lower())
        normalized = normalized.replace("_", " ")
        compact = normalized.replace(" ", "")
        canonical = alias_map.get(normalized) or alias_map.get(compact)
        if canonical and canonical not in df.columns:
            rename_map[column] = canonical

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _parse_numeric_value(value):
    if pd.isna(value):
        return float("nan")

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "-"}:
        return float("nan")

    negative = text.startswith("(") and text.endswith(")")
    cleaned = text.replace("(", "").replace(")", "")
    cleaned = cleaned.replace(",", "").replace(" ", "")
    cleaned = cleaned.replace("$", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)

    if cleaned in {"", "-", ".", "-."}:
        return float("nan")

    try:
        numeric = float(cleaned)
    except ValueError:
        return float("nan")

    return -numeric if negative and numeric > 0 else numeric


def _extract_currency_token(value):
    if pd.isna(value):
        return None

    text = str(value).strip().upper()
    if not text:
        return None

    if text in ALLOWED_CURRENCY_CODES:
        return text

    for code in ALLOWED_CURRENCY_CODES:
        if re.search(rf"\b{code}\b", text):
            return code

    return None


def _is_numeric_like(value):
    return pd.notna(_parse_numeric_value(value))


def _auto_correct_row_misalignment(df):
    required_cols = ["Currency", "Count", "ValueNett", "ValueDebit", "ValueCredit"]
    if any(col not in df.columns for col in required_cols):
        return df

    corrected_df = df.copy()
    for col in required_cols:
        corrected_df[col] = corrected_df[col].astype(object)
    correction_count = 0

    for idx in corrected_df.index:
        currency_value = corrected_df.at[idx, "Currency"]
        count_value = corrected_df.at[idx, "Count"]
        nett_value = corrected_df.at[idx, "ValueNett"]
        debit_value = corrected_df.at[idx, "ValueDebit"]
        credit_value = corrected_df.at[idx, "ValueCredit"]

        count_currency = _extract_currency_token(count_value)
        currency_currency = _extract_currency_token(currency_value)

        if count_currency and not currency_currency:
            if _is_numeric_like(currency_value):
                # Pattern: currency and count values swapped (e.g., Currency=10, Count=USD)
                corrected_df.at[idx, "Currency"] = count_currency
                corrected_df.at[idx, "Count"] = currency_value
                correction_count += 1
            else:
                # Pattern: one-cell shift to the right starting at Currency
                corrected_df.at[idx, "Currency"] = count_currency
                corrected_df.at[idx, "Count"] = nett_value
                corrected_df.at[idx, "ValueNett"] = debit_value
                corrected_df.at[idx, "ValueDebit"] = credit_value
                corrected_df.at[idx, "ValueCredit"] = float("nan")
                correction_count += 1

    corrected_df.attrs["auto_corrections_applied"] = correction_count
    return corrected_df


def _normalize_and_correct_dataframe(df):
    working_df = _canonicalize_columns(df)

    # Ensure required columns exist so downstream analysis remains stable.
    required_defaults = {
        "DateOfWeek": pd.NaT,
        "Product": "",
        "CompanyName": "",
        "Currency": "",
        "Count": 0,
        "ValueNett": 0,
        "ValueDebit": 0,
        "ValueCredit": 0,
    }
    for column_name, default_value in required_defaults.items():
        if column_name not in working_df.columns:
            working_df[column_name] = default_value

    working_df = _auto_correct_row_misalignment(working_df)

    for numeric_column in ["Count", "ValueNett", "ValueDebit", "ValueCredit"]:
        working_df[numeric_column] = working_df[numeric_column].apply(_parse_numeric_value)

    if "Currency" in working_df.columns:
        working_df["Currency"] = (
            working_df["Currency"]
            .apply(lambda value: _extract_currency_token(value) or "")
            .astype(str)
            .str.strip()
            .str.upper()
        )

    return working_df


def read_tabular_file(filepath):
    lower_path = str(filepath).lower()

    if lower_path.endswith(".csv"):
        return _normalize_and_correct_dataframe(pd.read_csv(filepath))

    engine_candidates = []
    if lower_path.endswith(".xlsx") or lower_path.endswith(".xlsm"):
        engine_candidates = ["openpyxl"]
    elif lower_path.endswith(".xls"):
        engine_candidates = ["xlrd", "openpyxl"]
    elif lower_path.endswith(".xlsb"):
        engine_candidates = ["pyxlsb"]
    else:
        # Unknown extension: try common Excel engines before final inference attempt.
        engine_candidates = ["openpyxl", "xlrd", "pyxlsb"]

    last_error = None
    for engine in engine_candidates:
        try:
            return _normalize_and_correct_dataframe(pd.read_excel(filepath, engine=engine))
        except Exception as exc:
            last_error = exc

    try:
        return _normalize_and_correct_dataframe(pd.read_excel(filepath))
    except Exception as exc:
        if last_error is None:
            last_error = exc

    raise ValueError(
        "Unable to read uploaded file as a supported spreadsheet. "
        "Please upload a valid .xlsx, .xls, .xlsb, or .csv file."
    ) from last_error


def _apply_date_window(df, days_window):
    if "DateOfWeek" not in df.columns or str(days_window).lower() == "all":
        return df

    working_df = df.copy()
    working_df["DateOfWeek"] = pd.to_datetime(working_df["DateOfWeek"], errors="coerce")
    dated_df = working_df.dropna(subset=["DateOfWeek"])

    if dated_df.empty:
        return df

    try:
        days = int(days_window)
    except (TypeError, ValueError):
        return df

    if days <= 0:
        return df

    latest_date = dated_df["DateOfWeek"].max()
    cutoff = latest_date - pd.Timedelta(days=days)

    filtered = dated_df[dated_df["DateOfWeek"] >= cutoff]
    return filtered if not filtered.empty else dated_df


def _apply_dimension_filters(df, product_filter, company_filter):
    filtered_df = df.copy()

    if "Product" in filtered_df.columns and str(product_filter).lower() != "all":
        filtered_df = filtered_df[filtered_df["Product"].astype(str) == str(product_filter)]

    if "CompanyName" in filtered_df.columns and str(company_filter).lower() != "all":
        filtered_df = filtered_df[filtered_df["CompanyName"].astype(str) == str(company_filter)]

    return filtered_df


def analyze_data(filepath, days_window="all", product_filter="all", company_filter="all"):
    df = read_tabular_file(filepath)
    correction_count = int(df.attrs.get("auto_corrections_applied", 0) or 0)

    # Clean numeric columns
    df["ValueDebit"] = pd.to_numeric(df["ValueDebit"], errors="coerce")
    df["ValueCredit"] = pd.to_numeric(df["ValueCredit"], errors="coerce")

    filtered_df = _apply_date_window(df, days_window)
    filtered_df = _apply_dimension_filters(filtered_df, product_filter, company_filter)
    filtered_df.attrs["auto_corrections_applied"] = correction_count

    results = {
        "total_transactions": len(filtered_df),
        "total_debit": filtered_df["ValueDebit"].sum(),
        "total_credit": filtered_df["ValueCredit"].sum(),
        "top_products": filtered_df["Product"].value_counts().head(5),
        "top_companies": filtered_df["CompanyName"].value_counts().head(5)
    }

    charts = generate_charts(filtered_df)

    return filtered_df, results, charts


def _prepare_comparison_dataset(filepath, days_window="all", product_filter="all", company_filter="all"):
    df = read_tabular_file(filepath)
    correction_count = int(df.attrs.get("auto_corrections_applied", 0) or 0)

    if "ValueDebit" in df.columns:
        df["ValueDebit"] = pd.to_numeric(df["ValueDebit"], errors="coerce")
    else:
        df["ValueDebit"] = 0

    if "ValueCredit" in df.columns:
        df["ValueCredit"] = pd.to_numeric(df["ValueCredit"], errors="coerce")
    else:
        df["ValueCredit"] = 0

    filtered_df = _apply_date_window(df, days_window)
    filtered_df = _apply_dimension_filters(filtered_df, product_filter, company_filter)
    filtered_df.attrs["auto_corrections_applied"] = correction_count

    return filtered_df


def split_comparison_items(df_a, df_b, compare_category):
    if not compare_category:
        return [], []

    categories_a = set(df_a[compare_category].dropna().astype(str).unique()) if compare_category in df_a.columns else set()
    categories_b = set(df_b[compare_category].dropna().astype(str).unique()) if compare_category in df_b.columns else set()

    # all_in_comparison: everything present in File B for current category
    all_in_comparison = sorted(categories_b)
    # shared_in_both: values present in both File A and File B
    shared_in_both = sorted(categories_a.intersection(categories_b))

    return all_in_comparison, shared_in_both


def get_comparison_item_sets(
    filepath_a,
    filepath_b,
    days_window="all",
    product_filter="all",
    company_filter="all",
    compare_category="Product"
):
    df_a = _prepare_comparison_dataset(filepath_a, days_window, product_filter, company_filter)
    df_b = _prepare_comparison_dataset(filepath_b, days_window, product_filter, company_filter)

    common_columns = sorted(set(df_a.columns).intersection(set(df_b.columns)))
    valid_category = compare_category if compare_category in common_columns else ""
    if not valid_category:
        valid_category = "Product" if "Product" in common_columns else "CompanyName" if "CompanyName" in common_columns else ""

    all_in_comparison, shared_in_both = split_comparison_items(df_a, df_b, valid_category)
    shared_lookup = set(shared_in_both)
    unique_in_comparison = [item for item in all_in_comparison if item not in shared_lookup]
    return {
        "compare_category": valid_category,
        "all_in_comparison": all_in_comparison,
        "shared_in_both": shared_in_both,
        "unique_in_comparison": unique_in_comparison
    }


def analyze_dataset_comparison(
    filepath_a,
    filepath_b,
    days_window="all",
    product_filter="all",
    company_filter="all",
    compare_columns=None,
    compare_category="Product",
    compare_items=None
):
    df_a = _prepare_comparison_dataset(filepath_a, days_window, product_filter, company_filter)
    df_b = _prepare_comparison_dataset(filepath_b, days_window, product_filter, company_filter)

    products_a = set(df_a["Product"].dropna().astype(str).unique()) if "Product" in df_a.columns else set()
    products_b = set(df_b["Product"].dropna().astype(str).unique()) if "Product" in df_b.columns else set()
    shared_products = sorted(products_a.intersection(products_b))

    companies_a = set(df_a["CompanyName"].dropna().astype(str).unique()) if "CompanyName" in df_a.columns else set()
    companies_b = set(df_b["CompanyName"].dropna().astype(str).unique()) if "CompanyName" in df_b.columns else set()
    shared_companies = sorted(companies_a.intersection(companies_b))

    common_columns = sorted(set(df_a.columns).intersection(set(df_b.columns)))

    default_compare_columns = [
        col for col in ["Count", "ValueNett"] if col in common_columns
    ]

    requested_columns = compare_columns if compare_columns else default_compare_columns
    selected_columns = [col for col in requested_columns if col in common_columns]
    if not selected_columns:
        selected_columns = default_compare_columns

    if not selected_columns:
        selected_columns = [
            col for col in common_columns
            if (
                pd.to_numeric(df_a[col], errors="coerce").notna().any()
                and pd.to_numeric(df_b[col], errors="coerce").notna().any()
            )
        ][:2]

    valid_category = compare_category if compare_category in common_columns else ""
    if not valid_category:
        valid_category = "Product" if "Product" in common_columns else "CompanyName" if "CompanyName" in common_columns else ""

    all_in_comparison, shared_in_both = split_comparison_items(df_a, df_b, valid_category)
    shared_lookup = set(shared_in_both)
    unique_in_comparison = [item for item in all_in_comparison if item not in shared_lookup]
    requested_items = [str(item) for item in (compare_items or [])]
    selected_compare_items = [item for item in requested_items if item in all_in_comparison]
    if not selected_compare_items:
        selected_compare_items = shared_in_both[:]

    column_comparisons = []
    for column_name in selected_columns:
        value_a = pd.to_numeric(df_a[column_name], errors="coerce").fillna(0).sum()
        value_b = pd.to_numeric(df_b[column_name], errors="coerce").fillna(0).sum()
        column_comparisons.append({
            "column": column_name,
            "file_a_total": value_a,
            "file_b_total": value_b,
            "delta": value_b - value_a
        })

    comparison_results = {
        "dataset_a_transactions": len(df_a),
        "dataset_b_transactions": len(df_b),
        "selected_compare_columns": selected_columns,
        "selected_compare_category": valid_category,
        "selected_compare_items": selected_compare_items,
        "all_in_comparison_count": len(all_in_comparison),
        "all_in_comparison_preview": all_in_comparison[:20],
        "shared_in_both_count": len(shared_in_both),
        "shared_in_both_preview": shared_in_both[:20],
        "unique_in_comparison_count": len(unique_in_comparison),
        "unique_in_comparison_preview": unique_in_comparison[:20],
        "column_comparisons": column_comparisons,
        "shared_products_count": len(shared_products),
        "shared_companies_count": len(shared_companies),
        "shared_products_preview": shared_products[:10],
        "shared_companies_preview": shared_companies[:10],
        "auto_corrections_file_a": int(df_a.attrs.get("auto_corrections_applied", 0) or 0),
        "auto_corrections_file_b": int(df_b.attrs.get("auto_corrections_applied", 0) or 0),
    }

    comparison_charts = generate_comparison_charts(
        df_a,
        df_b,
        shared_products,
        shared_companies,
        selected_columns,
        valid_category,
        selected_compare_items
    )

    return comparison_results, comparison_charts


def analyze_merchant(df, merchant_name):

    merchant_df = df[df["CompanyName"] == merchant_name]

    merchant_results = {
        "merchant_transactions": len(merchant_df),
        "merchant_total_debit": merchant_df["ValueDebit"].sum(),
        "merchant_total_credit": merchant_df["ValueCredit"].sum(),
        "merchant_top_products": merchant_df["Product"].value_counts().head(5)
    }

    return merchant_results


def generate_charts(df):

    charts = {}
    chart_font = "Aptos, Segoe UI, Calibri, Helvetica, Arial, sans-serif"

    def apply_clean_layout(fig, title):
        fig.update_layout(
            title=title,
            font={"family": chart_font, "color": "#e7efff"},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin={"l": 40, "r": 20, "t": 52, "b": 40},
            legend={"orientation": "h", "y": -0.2}
        )
        fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR)
        fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR)

    # Product popularity chart
    product_counts = df["Product"].value_counts().reset_index()
    product_counts.columns = ["Product", "Count"]

    fig1 = px.bar(
        product_counts.head(10),
        x="Product",
        y="Count",
        color_discrete_sequence=[BASE_COLOR]
    )
    apply_clean_layout(fig1, "Top Products (Bar)")
    fig1.add_hline(
        y=product_counts.head(10)["Count"].mean() if not product_counts.head(10).empty else 0,
        line_dash="dot",
        line_color=DELTA_COLOR,
        annotation_text="Average",
        annotation_position="top left"
    )

    charts["product_chart"] = _chart_html(fig1)


    # Product share pie (specific category composition)
    product_debit = (
        df.groupby("Product", dropna=False)["ValueDebit"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    top_n = 6
    if len(product_debit) > top_n:
        top_slice = product_debit.head(top_n).copy()
        other_total = product_debit.iloc[top_n:]["ValueDebit"].sum()
        top_slice.loc[len(top_slice)] = ["Other", other_total]
        pie_data = top_slice
    else:
        pie_data = product_debit

    fig_share = px.pie(
        pie_data,
        names="Product",
        values="ValueDebit",
        hole=0.45,
        color_discrete_sequence=[BASE_COLOR, COMPARE_COLOR, DELTA_COLOR, ALERT_COLOR, "#9bdbff", "#90f0bd", "#ffd88b"]
    )
    apply_clean_layout(fig_share, "Debit Share by Product (Pie)")
    fig_share.update_traces(textposition="inside", textinfo="percent+label")
    charts["share_chart"] = _chart_html(fig_share)


    # Merchant comparison
    merchant_totals = (
        df.groupby("CompanyName", dropna=False)["ValueDebit"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    fig2 = px.bar(
        merchant_totals,
        x="ValueDebit",
        y="CompanyName",
        orientation="h",
        color_discrete_sequence=[BASE_COLOR]
    )
    apply_clean_layout(fig2, "Top Merchants by Debit (Scale)")
    fig2.update_yaxes(categoryorder="total ascending")

    charts["merchant_chart"] = _chart_html(fig2)


    # Transaction scale buckets (specific value bands)
    clean_debit = df["ValueDebit"].dropna()
    scale_labels = ["Low", "Medium", "High", "Very High"]

    if clean_debit.nunique() >= 4:
        try:
            scale_bins = pd.qcut(clean_debit, q=4, labels=scale_labels)
        except ValueError:
            # Fallback for heavily duplicated values where quantile bins collapse.
            scale_bins = pd.cut(clean_debit, bins=4, labels=scale_labels)

        scale_counts = (
            scale_bins.value_counts()
            .reindex(scale_labels, fill_value=0)
            .reset_index()
        )
    else:
        scale_counts = pd.DataFrame({"Scale": scale_labels, "Count": [0, 0, 0, 0]})

    scale_counts.columns = ["Scale", "Count"]

    fig3 = px.bar(
        scale_counts,
        x="Scale",
        y="Count",
        color_discrete_sequence=[BASE_COLOR]
    )
    apply_clean_layout(fig3, "Transaction Value Scale (Bar)")

    charts["revenue_chart"] = _chart_html(fig3)


    # Transaction trend
    if "DateOfWeek" in df.columns:

        # Convert column safely to datetime
        df["DateOfWeek"] = pd.to_datetime(df["DateOfWeek"], errors="coerce")

        # Remove invalid dates
        trend_df = df.dropna(subset=["DateOfWeek"])

        # Group transactions by date
        trend = (
            trend_df.groupby("DateOfWeek")["ValueDebit"]
            .sum()
            .sort_index()
            .reset_index()
        )

        fig4 = px.line(
            trend,
            x="DateOfWeek",
            y="ValueDebit",
            markers=True
        )
        apply_clean_layout(fig4, "Debit Trend Over Time")
        if not trend.empty:
            fig4.add_hline(
                y=trend["ValueDebit"].mean(),
                line_dash="dot",
                line_color=DELTA_COLOR,
                annotation_text="Average",
                annotation_position="top left"
            )
        fig4.update_traces(line={"color": BASE_COLOR})

        charts["trend_chart"] = _chart_html(fig4)

    return charts


def generate_comparison_charts(
    df_a,
    df_b,
    shared_products,
    shared_companies,
    selected_columns,
    compare_category,
    selected_compare_items=None
):
    charts = {}
    chart_font = "Aptos, Segoe UI, Calibri, Helvetica, Arial, sans-serif"

    def apply_clean_layout(fig, title):
        fig.update_layout(
            title=title,
            font={"family": chart_font, "color": "#e7efff"},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin={"l": 40, "r": 20, "t": 52, "b": 40},
            legend={"orientation": "h", "y": -0.2}
        )
        fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR)
        fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR)

    if selected_columns:
        metric_rows = []
        for metric in selected_columns:
            metric_rows.append({
                "Metric": metric,
                "Dataset": "File A",
                "Value": pd.to_numeric(df_a[metric], errors="coerce").fillna(0).sum()
            })
            metric_rows.append({
                "Metric": metric,
                "Dataset": "File B",
                "Value": pd.to_numeric(df_b[metric], errors="coerce").fillna(0).sum()
            })

        metrics_df = pd.DataFrame(metric_rows)
        fig_metric_totals = px.bar(
            metrics_df,
            x="Metric",
            y="Value",
            color="Dataset",
            barmode="group",
            color_discrete_sequence=[BASE_COLOR, COMPARE_COLOR]
        )
        apply_clean_layout(fig_metric_totals, "Selected Columns: File A vs File B")
        if not metrics_df.empty:
            fig_metric_totals.add_hline(
                y=metrics_df["Value"].mean(),
                line_dash="dot",
                line_color=DELTA_COLOR,
                annotation_text="Average",
                annotation_position="top left"
            )
        charts["compare_selected_columns_chart"] = _chart_html(fig_metric_totals)

    if (
        compare_category
        and compare_category in df_a.columns
        and compare_category in df_b.columns
        and selected_columns
    ):
        categories_a = set(df_a[compare_category].dropna().astype(str).unique())
        categories_b = set(df_b[compare_category].dropna().astype(str).unique())
        shared_categories = sorted(categories_a.intersection(categories_b))
        scoped_categories = [item for item in (selected_compare_items or []) if item in categories_b]
        categories_for_chart = scoped_categories if scoped_categories else shared_categories

        category_rows = []
        for category_value in categories_for_chart[:10]:
            for metric in selected_columns:
                metric_a = pd.to_numeric(
                    df_a[df_a[compare_category].astype(str) == category_value][metric],
                    errors="coerce"
                ).fillna(0).sum()
                metric_b = pd.to_numeric(
                    df_b[df_b[compare_category].astype(str) == category_value][metric],
                    errors="coerce"
                ).fillna(0).sum()

                category_rows.append({
                    "Category": category_value,
                    "Metric": metric,
                    "Dataset": "File A",
                    "Value": metric_a
                })
                category_rows.append({
                    "Category": category_value,
                    "Metric": metric,
                    "Dataset": "File B",
                    "Value": metric_b
                })

        if category_rows:
            category_df = pd.DataFrame(category_rows)
            fig_category = px.bar(
                category_df,
                x="Category",
                y="Value",
                color="Dataset",
                barmode="group",
                facet_row="Metric",
                color_discrete_sequence=[BASE_COLOR, COMPARE_COLOR]
            )
            apply_clean_layout(fig_category, f"Shared {compare_category}: Metric Comparison")
            charts["compare_selected_category_chart"] = _chart_html(fig_category)

    if shared_products and "Product" in df_a.columns and "Product" in df_b.columns:
        compare_products = []
        for product in shared_products[:10]:
            debit_a = df_a[df_a["Product"].astype(str) == product]["ValueDebit"].sum()
            debit_b = df_b[df_b["Product"].astype(str) == product]["ValueDebit"].sum()
            compare_products.append({"Product": product, "Dataset": "File A", "ValueDebit": debit_a})
            compare_products.append({"Product": product, "Dataset": "File B", "ValueDebit": debit_b})

        product_df = pd.DataFrame(compare_products)
        fig_products = px.bar(
            product_df,
            x="Product",
            y="ValueDebit",
            color="Dataset",
            barmode="group",
            color_discrete_sequence=[BASE_COLOR, COMPARE_COLOR]
        )
        apply_clean_layout(fig_products, "Shared Products: Debit Comparison")
        charts["compare_product_chart"] = _chart_html(fig_products)

    if shared_companies and "CompanyName" in df_a.columns and "CompanyName" in df_b.columns:
        compare_companies = []
        for company in shared_companies[:10]:
            debit_a = df_a[df_a["CompanyName"].astype(str) == company]["ValueDebit"].sum()
            debit_b = df_b[df_b["CompanyName"].astype(str) == company]["ValueDebit"].sum()
            compare_companies.append({"CompanyName": company, "Dataset": "File A", "ValueDebit": debit_a})
            compare_companies.append({"CompanyName": company, "Dataset": "File B", "ValueDebit": debit_b})

        company_df = pd.DataFrame(compare_companies)
        fig_companies = px.bar(
            company_df,
            x="ValueDebit",
            y="CompanyName",
            color="Dataset",
            orientation="h",
            barmode="group",
            color_discrete_sequence=[BASE_COLOR, COMPARE_COLOR]
        )
        apply_clean_layout(fig_companies, "Shared Companies: Debit Comparison")
        fig_companies.update_yaxes(categoryorder="total ascending")
        charts["compare_company_chart"] = _chart_html(fig_companies)

    if "DateOfWeek" in df_a.columns and "DateOfWeek" in df_b.columns:
        trend_a = df_a.copy()
        trend_b = df_b.copy()

        trend_a["DateOfWeek"] = pd.to_datetime(trend_a["DateOfWeek"], errors="coerce")
        trend_b["DateOfWeek"] = pd.to_datetime(trend_b["DateOfWeek"], errors="coerce")

        trend_a = trend_a.dropna(subset=["DateOfWeek"])
        trend_b = trend_b.dropna(subset=["DateOfWeek"])

        if not trend_a.empty or not trend_b.empty:
            trend_series_a = trend_a.groupby("DateOfWeek")["ValueDebit"].sum()
            trend_series_b = trend_b.groupby("DateOfWeek")["ValueDebit"].sum()

            trend_index = trend_series_a.index.union(trend_series_b.index)
            trend_compare_df = pd.DataFrame({
                "DateOfWeek": trend_index,
                "File A": trend_series_a.reindex(trend_index, fill_value=0).values,
                "File B": trend_series_b.reindex(trend_index, fill_value=0).values
            }).sort_values("DateOfWeek")

            fig_trend = go.Figure()
            fig_trend.add_trace(
                go.Scatter(
                    x=trend_compare_df["DateOfWeek"],
                    y=trend_compare_df["File A"],
                    mode="lines+markers",
                    name="File A",
                    line={"color": BASE_COLOR}
                )
            )
            fig_trend.add_trace(
                go.Scatter(
                    x=trend_compare_df["DateOfWeek"],
                    y=trend_compare_df["File B"],
                    mode="lines+markers",
                    name="File B",
                    line={"color": COMPARE_COLOR}
                )
            )
            apply_clean_layout(fig_trend, "Debit Trend Comparison")
            if not trend_compare_df.empty:
                combined_average = (trend_compare_df["File A"].mean() + trend_compare_df["File B"].mean()) / 2
                fig_trend.add_hline(
                    y=combined_average,
                    line_dash="dot",
                    line_color=DELTA_COLOR,
                    annotation_text="Average",
                    annotation_position="top left"
                )
            charts["compare_trend_chart"] = _chart_html(fig_trend)

    return charts