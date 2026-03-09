import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def read_tabular_file(filepath):
    lower_path = str(filepath).lower()

    if lower_path.endswith(".csv"):
        return pd.read_csv(filepath)

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
            return pd.read_excel(filepath, engine=engine)
        except Exception as exc:
            last_error = exc

    try:
        return pd.read_excel(filepath)
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

    # Clean numeric columns
    df["ValueDebit"] = pd.to_numeric(df["ValueDebit"], errors="coerce")
    df["ValueCredit"] = pd.to_numeric(df["ValueCredit"], errors="coerce")

    filtered_df = _apply_date_window(df, days_window)
    filtered_df = _apply_dimension_filters(filtered_df, product_filter, company_filter)

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

    return filtered_df


def analyze_dataset_comparison(
    filepath_a,
    filepath_b,
    days_window="all",
    product_filter="all",
    company_filter="all",
    compare_columns=None,
    compare_category="Product"
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
        "column_comparisons": column_comparisons,
        "shared_products_count": len(shared_products),
        "shared_companies_count": len(shared_companies),
        "shared_products_preview": shared_products[:10],
        "shared_companies_preview": shared_companies[:10]
    }

    comparison_charts = generate_comparison_charts(
        df_a,
        df_b,
        shared_products,
        shared_companies,
        selected_columns,
        valid_category
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
        fig.update_xaxes(showgrid=True, gridcolor="rgba(190,210,255,0.14)")
        fig.update_yaxes(showgrid=True, gridcolor="rgba(190,210,255,0.14)")

    # Product popularity chart
    product_counts = df["Product"].value_counts().reset_index()
    product_counts.columns = ["Product", "Count"]

    fig1 = px.bar(
        product_counts.head(10),
        x="Product",
        y="Count",
        color="Count",
        color_continuous_scale="Teal"
    )
    apply_clean_layout(fig1, "Top Products (Bar)")
    fig1.update_layout(coloraxis_showscale=False)

    charts["product_chart"] = fig1.to_html(full_html=False)


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
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    apply_clean_layout(fig_share, "Debit Share by Product (Pie)")
    fig_share.update_traces(textposition="inside", textinfo="percent+label")
    charts["share_chart"] = fig_share.to_html(full_html=False)


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
        color="ValueDebit",
        color_continuous_scale="Blues"
    )
    apply_clean_layout(fig2, "Top Merchants by Debit (Scale)")
    fig2.update_layout(coloraxis_showscale=False)
    fig2.update_yaxes(categoryorder="total ascending")

    charts["merchant_chart"] = fig2.to_html(full_html=False)


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
        color="Count",
        color_continuous_scale="Emrld"
    )
    apply_clean_layout(fig3, "Transaction Value Scale (Bar)")
    fig3.update_layout(coloraxis_showscale=False)

    charts["revenue_chart"] = fig3.to_html(full_html=False)


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

        charts["trend_chart"] = fig4.to_html(full_html=False)

    return charts


def generate_comparison_charts(
    df_a,
    df_b,
    shared_products,
    shared_companies,
    selected_columns,
    compare_category
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
        fig.update_xaxes(showgrid=True, gridcolor="rgba(190,210,255,0.14)")
        fig.update_yaxes(showgrid=True, gridcolor="rgba(190,210,255,0.14)")

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
            color_discrete_sequence=["#3cc6ff", "#2dd37f"]
        )
        apply_clean_layout(fig_metric_totals, "Selected Columns: File A vs File B")
        charts["compare_selected_columns_chart"] = fig_metric_totals.to_html(full_html=False)

    if (
        compare_category
        and compare_category in df_a.columns
        and compare_category in df_b.columns
        and selected_columns
    ):
        categories_a = set(df_a[compare_category].dropna().astype(str).unique())
        categories_b = set(df_b[compare_category].dropna().astype(str).unique())
        shared_categories = sorted(categories_a.intersection(categories_b))

        category_rows = []
        for category_value in shared_categories[:10]:
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
                color_discrete_sequence=["#3cc6ff", "#2dd37f"]
            )
            apply_clean_layout(fig_category, f"Shared {compare_category}: Metric Comparison")
            charts["compare_selected_category_chart"] = fig_category.to_html(full_html=False)

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
            color_discrete_sequence=["#3cc6ff", "#2dd37f"]
        )
        apply_clean_layout(fig_products, "Shared Products: Debit Comparison")
        charts["compare_product_chart"] = fig_products.to_html(full_html=False)

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
            color_discrete_sequence=["#3cc6ff", "#2dd37f"]
        )
        apply_clean_layout(fig_companies, "Shared Companies: Debit Comparison")
        fig_companies.update_yaxes(categoryorder="total ascending")
        charts["compare_company_chart"] = fig_companies.to_html(full_html=False)

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
                    line={"color": "#3cc6ff"}
                )
            )
            fig_trend.add_trace(
                go.Scatter(
                    x=trend_compare_df["DateOfWeek"],
                    y=trend_compare_df["File B"],
                    mode="lines+markers",
                    name="File B",
                    line={"color": "#2dd37f"}
                )
            )
            apply_clean_layout(fig_trend, "Debit Trend Comparison")
            charts["compare_trend_chart"] = fig_trend.to_html(full_html=False)

    return charts