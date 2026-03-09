from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_cors import CORS
import os
import pandas as pd
from datetime import timedelta, datetime
from werkzeug.utils import secure_filename
from analyzer import analyze_data, analyze_merchant, analyze_dataset_comparison, read_tabular_file


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


def load_comparison_options(filepath_a, filepath_b):
    if not filepath_a or not filepath_b:
        return ["Count", "ValueNett"], ["Product", "CompanyName"]

    if not os.path.exists(filepath_a) or not os.path.exists(filepath_b):
        return ["Count", "ValueNett"], ["Product", "CompanyName"]

    df_a = read_tabular_file(filepath_a)
    df_b = read_tabular_file(filepath_b)

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
    available_compare_columns = preferred_compare_columns if preferred_compare_columns else numeric_candidate_columns

    preferred_categories = [
        column_name for column_name in ["Product", "CompanyName"] if column_name in common_columns
    ]
    available_categories = preferred_categories if preferred_categories else common_columns

    return available_compare_columns, available_categories


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

    source_df = read_tabular_file(filepath)
    product_options = sorted(source_df["Product"].dropna().astype(str).unique()) if "Product" in source_df.columns else []
    company_options = sorted(source_df["CompanyName"].dropna().astype(str).unique()) if "CompanyName" in source_df.columns else []

    return product_options, company_options


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

    host = req.host.split(":", 1)[0] if req.host else "127.0.0.1"
    return f"http://{host}:3000"


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        # Always use the React login screen.
        return redirect(resolve_react_login_url(request))

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

        return redirect(url_for("login"))


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
    upload_error = None

    product_options = None
    company_options = None

    if latest_upload_path_global and os.path.exists(latest_upload_path_global):
        try:
            product_options, company_options = load_filter_options(latest_upload_path_global)
        except ValueError as exc:
            upload_error = str(exc)
            product_options, company_options = [], []

    if (
        latest_upload_path_global
        and secondary_upload_path_global
        and os.path.exists(latest_upload_path_global)
        and os.path.exists(secondary_upload_path_global)
    ):
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
        except ValueError as exc:
            upload_error = str(exc)

    if request.method == "POST":
        selected_window = request.form.get("date_window", selected_window_global)
        selected_product = request.form.get("product_filter", selected_product_global)
        selected_company = request.form.get("company_filter", selected_company_global)
        selected_compare_columns = request.form.getlist("compare_columns")
        selected_compare_category = request.form.get("compare_category", selected_compare_category_global)

        selected_window_global = selected_window
        selected_product_global = selected_product
        selected_company_global = selected_company

        has_new_upload = False

        # Primary file upload (File A)
        primary_file = request.files.get("file_primary") or request.files.get("file")
        if primary_file and primary_file.filename != "":
            latest_upload_path_global = save_uploaded_file(primary_file, "primary")
            has_new_upload = True

        # Secondary file upload (File B)
        secondary_file = request.files.get("file_secondary")
        if secondary_file and secondary_file.filename != "":
            secondary_upload_path_global = save_uploaded_file(secondary_file, "secondary")
            has_new_upload = True

        if (
            latest_upload_path_global
            and secondary_upload_path_global
            and os.path.exists(latest_upload_path_global)
            and os.path.exists(secondary_upload_path_global)
        ):
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

            selected_compare_columns_global = selected_compare_columns
            selected_compare_category_global = selected_compare_category

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

                merchants = df_global["CompanyName"].dropna().unique() if "CompanyName" in df_global.columns else []

                if secondary_upload_path_global and os.path.exists(secondary_upload_path_global):
                    comparison_results_global, comparison_charts_global = analyze_dataset_comparison(
                        latest_upload_path_global,
                        secondary_upload_path_global,
                        selected_window,
                        selected_product,
                        selected_company,
                        selected_compare_columns,
                        selected_compare_category
                    )
                    comparison_results = comparison_results_global
                    comparison_charts = comparison_charts_global
                else:
                    comparison_results_global = None
                    comparison_charts_global = None
                    comparison_results = None
                    comparison_charts = None
            except ValueError as exc:
                upload_error = str(exc)

        if latest_upload_path_global and os.path.exists(latest_upload_path_global):
            try:
                product_options, company_options = load_filter_options(latest_upload_path_global)
            except ValueError as exc:
                upload_error = str(exc)
                product_options, company_options = [], []

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
        upload_error=upload_error,
        has_primary_upload=bool(latest_upload_path_global and os.path.exists(latest_upload_path_global)),
        has_secondary_upload=bool(secondary_upload_path_global and os.path.exists(secondary_upload_path_global)),
        primary_filename=os.path.basename(latest_upload_path_global) if latest_upload_path_global and os.path.exists(latest_upload_path_global) else None,
        secondary_filename=os.path.basename(secondary_upload_path_global) if secondary_upload_path_global and os.path.exists(secondary_upload_path_global) else None
    )


if __name__ == "__main__":
    app.run(debug=True)