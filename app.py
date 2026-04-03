import os
import json
import logging
import secrets
import re
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
from decimal import Decimal
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, abort
from datetime import datetime, date
import db
import analysis

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

_secret_key = os.environ.get("SECRET_KEY")
if not _secret_key:
    # Use an ephemeral key in local/dev if SECRET_KEY is not configured.
    _secret_key = secrets.token_hex(32)
    app.logger.warning("SECRET_KEY is not set. Using an ephemeral key for this process.")

app.secret_key = _secret_key
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_ENV") == "production",
)

PROFILES = ["Federico", "Anna"]

CATEGORIES = {
    "Expense": [
        "Food", "Housing", "Transport", "Car", "Health", "Personal",
        "Clothing", "Entertainment", "Gifts", "Bimbo", "Finance",
        "Travel", "Utilities", "Education", "Other"
    ],
    "Income": ["Salary", "Bonus", "Investment", "Other"]
}

SUBCATEGORIES = {
    "Food":          ["Groceries", "Dinner", "Lunch", "Breakfast", "Bar", ""],
    "Housing":       ["Rent", "Mortgage", "Utilities", "Maintenance", "Decorating", ""],
    "Transport":     ["Public", "Taxi", "Fuel", ""],
    "Car":           ["Insurance", "Fuel", "Maintenance", "Autostrada", ""],
    "Health":        ["Doctor", "Medications", "Hospital", "Dentist", ""],
    "Personal":      ["Cosmetics", "Estetista", "Haircut", ""],
    "Clothing":      ["Shoes", "Pants", "Shirt", "Dress", ""],
    "Entertainment": ["Cinema", "Games", "Books", "Sports", ""],
    "Gifts":         ["Birthday", "Christmas", "Nascite", ""],
    "Bimbo":         ["Asilo", "Clothes", "Toys", "Food", ""],
    "Finance":       ["Fineco", "Conto deposito", ""],
    "Travel":        ["Hotel", "Flight", "Activities", ""],
    "Utilities":     ["Electricity", "Gas", "Water", "Internet", "Phone", ""],
    "Education":     ["Course", "Books", ""],
    "Salary":        [""],
    "Bonus":         [""],
    "Investment":    ["Dividends", "Capital Gains", ""],
    "Other":         [""],
}

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
VALID_CURRENCIES = {"EUR", "USD", "GBP", "CHF"}


@app.after_request
def set_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "font-src 'self' https://cdn.jsdelivr.net; "
        "connect-src 'self'; "
        "frame-ancestors 'none';"
    )
    return response


def _get_csrf_token():
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


@app.context_processor
def inject_csrf_token():
    return {"csrf_token": _get_csrf_token}


@app.before_request
def enforce_csrf():
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        if request.endpoint == "static":
            return
        expected = session.get("csrf_token")
        provided = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
        if not expected or not provided or not secrets.compare_digest(expected, provided):
            app.logger.warning("CSRF check failed for endpoint '%s'", request.endpoint)
            abort(400, description="Invalid CSRF token")


def _validate_tx_form(data):
    profile = data.get("profile", "")
    tx_type = data.get("type", "")
    category = data.get("category", "")
    subcategory = data.get("subcategory", "")
    tx_date = data.get("date", "")
    currency = data.get("currency", "EUR")
    notes = (data.get("notes", "") or "").strip()

    if profile not in PROFILES:
        return "Profilo non valido.", None
    if tx_type not in CATEGORIES:
        return "Tipo transazione non valido.", None
    if category not in CATEGORIES[tx_type]:
        return "Categoria non valida per il tipo selezionato.", None
    if currency not in VALID_CURRENCIES:
        return "Valuta non valida.", None
    if len(notes) > 500:
        return "Le note sono troppo lunghe (max 500 caratteri).", None

    allowed_subcats = set(SUBCATEGORIES.get(category, [""]))
    if subcategory not in allowed_subcats:
        return "Sottocategoria non valida.", None

    try:
        parsed_date = datetime.strptime(tx_date, "%Y-%m-%d").date()
    except ValueError:
        return "Data non valida.", None

    try:
        amount = float(data.get("amount", ""))
        amount_eur_raw = data.get("amount_eur")
        amount_eur = float(amount_eur_raw) if amount_eur_raw else amount
    except (TypeError, ValueError):
        return "Importi non validi.", None

    if amount <= 0 or amount_eur <= 0:
        return "Gli importi devono essere maggiori di zero.", None

    return None, {
        "profile": profile,
        "type": tx_type,
        "category": category,
        "subcategory": subcategory,
        "date": parsed_date.isoformat(),
        "currency": currency,
        "amount": amount,
        "amount_eur": amount_eur,
        "notes": notes,
    }


def convert_row(row):
    result = {}
    for k, v in row.items():
        result[k] = float(v) if isinstance(v, Decimal) else v
    return result


def make_chart(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", transparent=True)
    buf.seek(0)
    img = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return img


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    config          = analysis.load_config()
    params          = config["params"]
    initial_liquidity = params["liquidita_31_giu_22"]

    df = analysis.get_transactions_df()

    # ---- financial metrics (Finance excluded from averages) ----
    avg_income, avg_expense, avg_savings = analysis.last_n_months_avg(df, 12)
    inflation = analysis.calculate_personal_inflation(df)

    # ---- investment tracking ----
    fineco_invested        = analysis.compute_fineco_invested(df)
    conto_dep_invested     = analysis.compute_conto_deposito_invested(df)
    total_invested_capital = fineco_invested + conto_dep_invested

    # bank account balance (all cash flows, including finance transfers as outflows)
    bank_balance      = analysis.savings_balance(df, initial_liquidity, config)
    current_liquidity = bank_balance - total_invested_capital

    # ideal liquidity targets
    ideal_liquidity   = avg_expense * params.get("mesi_di_spese_correnti", 2)
    deposit_ideal     = avg_expense * params.get("mesi_di_spese_impreviste", 5)
    investable_amount = max(current_liquidity - ideal_liquidity, 0)

    # ---- savings trend chart (real bank balance over time) ----
    trend_df    = analysis.savings_trend(df, initial_liquidity, config)
    chart_savings = None
    if not trend_df.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(trend_df["YearMonth"].astype(str), trend_df["CumulativeSavings"],
                color="#4CAF50", linewidth=2, label="Bilancio bancario")
        ax.fill_between(range(len(trend_df)), trend_df["CumulativeSavings"],
                        alpha=0.08, color="#4CAF50")
        step = max(1, len(trend_df) // 8)
        ax.set_xticks(range(0, len(trend_df), step))
        ax.set_xticklabels(trend_df["YearMonth"].astype(str).iloc[::step],
                           rotation=45, ha="right", fontsize=8)
        ax.set_title("Andamento Bilancio Bancario", fontweight="bold")
        ax.set_ylabel("EUR")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_savings = make_chart(fig)

    # ---- monthly income/expense chart (real expenses, Finance excluded) ----
    summary = analysis.monthly_summary(df, exclude_investments=True)
    recent  = summary.head(6).to_dict("records") if not summary.empty else []

    chart_monthly = None
    if not summary.empty:
        last12 = summary.sort_values("YearMonth").tail(12)
        x = range(len(last12))
        fig, ax = plt.subplots(figsize=(10, 4))
        w = 0.35
        ax.bar([i - w/2 for i in x], last12["Income"],  w, label="Entrate",      color="#2196F3", alpha=0.85)
        ax.bar([i + w/2 for i in x], last12["Expense"], w, label="Spese reali",  color="#F44336", alpha=0.85)
        ax.plot(list(x), last12["Savings"], color="#4CAF50", marker="o", linewidth=2, label="Risparmio")
        ax.set_xticks(list(x))
        ax.set_xticklabels(last12["YearMonth"].astype(str), rotation=45, ha="right", fontsize=8)
        ax.set_title("Ultimi 12 Mesi – Entrate / Spese Reali / Risparmio", fontweight="bold")
        ax.set_ylabel("EUR")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_monthly = make_chart(fig)

    # ---- per-profile totals ----
    anna_df = df[df["profile"] == "Anna"]
    fede_df = df[df["profile"] == "Federico"]

    def profile_totals(pdf):
        income     = float(pdf[pdf["type"] == "Income"]["amount_eur"].sum())
        all_exp    = float(pdf[pdf["type"] == "Expense"]["amount_eur"].sum())
        invested   = float(pdf[pdf.apply(analysis.is_investment_transfer, axis=1)]["amount_eur"].sum())
        real_exp   = all_exp - invested
        return income, real_exp, invested

    anna_income, anna_expense, anna_invested = profile_totals(anna_df)
    fede_income, fede_expense, fede_invested = profile_totals(fede_df)

    current_month = datetime.now().strftime("%Y-%m")
    current_month_data = summary[summary["YearMonth"] == current_month].to_dict("records")
    current_month_stats = current_month_data[0] if current_month_data else None

    return render_template("dashboard.html",
        recent=recent,
        avg_income=avg_income,
        avg_expense=avg_expense,
        avg_savings=avg_savings,
        inflation=inflation,
        bank_balance=round(bank_balance, 2),
        current_liquidity=round(current_liquidity, 2),
        fineco_invested=round(fineco_invested, 2),
        conto_dep_invested=round(conto_dep_invested, 2),
        total_invested_capital=round(total_invested_capital, 2),
        ideal_liquidity=round(ideal_liquidity, 2),
        deposit_ideal=round(deposit_ideal, 2),
        investable_amount=round(investable_amount, 2),
        chart_savings=chart_savings,
        chart_monthly=chart_monthly,
        anna_income=anna_income,
        anna_expense=anna_expense,
        anna_invested=anna_invested,
        fede_income=fede_income,
        fede_expense=fede_expense,
        fede_invested=fede_invested,
        current_month_stats=current_month_stats,
    )


@app.route("/transactions")
def transactions():
    profile = request.args.get("profile", "")
    tx_type = request.args.get("type", "")
    category = request.args.get("category", "")
    month = request.args.get("month", "")

    if profile and profile not in PROFILES:
        profile = ""
    if tx_type and tx_type not in {"Expense", "Income"}:
        tx_type = ""
    valid_categories = set(CATEGORIES["Expense"] + CATEGORIES["Income"])
    if category and category not in valid_categories:
        category = ""
    if month and not MONTH_PATTERN.match(month):
        month = ""

    sql    = "SELECT * FROM transactions WHERE 1=1"
    params = []
    if profile:  sql += " AND profile = %s";                 params.append(profile)
    if tx_type:  sql += " AND type = %s";                    params.append(tx_type)
    if category: sql += " AND category = %s";                params.append(category)
    if month:    sql += " AND TO_CHAR(date, 'YYYY-MM') = %s"; params.append(month)
    sql += " ORDER BY date DESC LIMIT 200"

    rows = db.query(sql, params)
    txs  = [convert_row(dict(r)) for r in rows] if rows else []

    cats     = db.query("SELECT DISTINCT category FROM transactions ORDER BY category")
    all_cats = [r["category"] for r in cats] if cats else []

    return render_template("transactions.html",
        transactions=txs,
        profiles=PROFILES,
        categories=all_cats,
        sel_profile=profile,
        sel_type=tx_type,
        sel_category=category,
        sel_month=month,
    )


@app.route("/add", methods=["GET", "POST"])
def add_transaction():
    if request.method == "POST":
        error, clean = _validate_tx_form(request.form)
        if error:
            flash(error, "danger")
            return redirect(url_for("add_transaction"))

        try:
            db.execute(
                """INSERT INTO transactions
                   (date, profile, category, subcategory, amount, currency, amount_eur, type, notes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [
                    clean["date"], clean["profile"], clean["category"], clean["subcategory"],
                    clean["amount"], clean["currency"], clean["amount_eur"], clean["type"], clean["notes"]
                ]
            )
            flash(f"Transazione aggiunta con successo per {clean['profile']}!", "success")
            return redirect(url_for("transactions"))
        except Exception:
            app.logger.exception("Failed to insert transaction")
            flash("Errore durante il salvataggio. Riprova più tardi.", "danger")
            return redirect(url_for("add_transaction"))

    return render_template("add_transaction.html",
        profiles=PROFILES,
        categories=CATEGORIES,
        subcategories=SUBCATEGORIES,
        today=date.today().isoformat(),
    )


@app.route("/edit/<int:tx_id>", methods=["GET", "POST"])
def edit_transaction(tx_id):
    if request.method == "POST":
        error, clean = _validate_tx_form(request.form)
        if error:
            flash(error, "danger")
            return redirect(url_for("edit_transaction", tx_id=tx_id))

        try:
            db.execute(
                """UPDATE transactions SET date=%s, profile=%s, category=%s, subcategory=%s,
                   amount=%s, currency=%s, amount_eur=%s, type=%s, notes=%s WHERE id=%s""",
                [
                    clean["date"], clean["profile"], clean["category"], clean["subcategory"],
                    clean["amount"], clean["currency"], clean["amount_eur"], clean["type"], clean["notes"], tx_id
                ]
            )
            flash("Transazione aggiornata con successo!", "success")
            return redirect(url_for("transactions"))
        except Exception:
            app.logger.exception("Failed to update transaction id=%s", tx_id)
            flash("Errore durante l'aggiornamento. Riprova più tardi.", "danger")
            return redirect(url_for("edit_transaction", tx_id=tx_id))

    row = db.query("SELECT * FROM transactions WHERE id = %s", [tx_id], fetchall=False)
    if not row:
        flash("Transazione non trovata.", "danger")
        return redirect(url_for("transactions"))
    return render_template("edit_transaction.html",
        tx=convert_row(dict(row)),
        profiles=PROFILES,
        categories=CATEGORIES,
        subcategories=SUBCATEGORIES,
    )


@app.route("/delete/<int:tx_id>", methods=["POST"])
def delete_transaction(tx_id):
    try:
        db.execute("DELETE FROM transactions WHERE id = %s", [tx_id])
        flash("Transazione eliminata.", "success")
    except Exception:
        app.logger.exception("Failed to delete transaction id=%s", tx_id)
        flash("Errore durante l'eliminazione. Riprova più tardi.", "danger")
    return redirect(url_for("transactions"))


@app.route("/reports")
def reports():
    config    = analysis.load_config()
    df        = analysis.get_transactions_df()
    anomalous = analysis.get_anomalous_months(config)

    # averages over 18 months, Finance excluded
    avg_income, avg_expense, avg_savings = analysis.last_n_months_avg(df, 18)
    inflation = analysis.calculate_personal_inflation(df)

    # full monthly summary (Finance excluded from Expense column)
    summary      = analysis.monthly_summary(df, exclude_investments=True)
    full_summary = summary.sort_values("YearMonth").to_dict("records") if not summary.empty else []

    # ---- category chart (Finance shown separately for transparency) ----
    chart_cat = None
    if not df.empty:
        cat_df = analysis.category_breakdown(df, exclude_investments=True)
        if not cat_df.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            colors = plt.cm.Set3(np.linspace(0, 1, len(cat_df)))
            bars = ax.barh(cat_df["category"], cat_df["amount_eur"], color=colors)
            ax.set_title("Spese Reali per Categoria (Totale Storico, senza Finance)",
                         fontweight="bold")
            ax.set_xlabel("EUR")
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
            for bar, val in zip(bars, cat_df["amount_eur"]):
                ax.text(bar.get_width() + 10, bar.get_y() + bar.get_height() / 2,
                        f"€{val:,.0f}", va="center", fontsize=8)
            fig.tight_layout()
            chart_cat = make_chart(fig)

    # ---- investment vs real spending chart ----
    chart_invest_vs_spend = None
    if not df.empty:
        invest_monthly = (df[df.apply(analysis.is_investment_transfer, axis=1)]
                          .groupby("YearMonth")["amount_eur"].sum())
        real_monthly   = (df[(df["type"] == "Expense") &
                             ~df.apply(analysis.is_investment_transfer, axis=1)]
                          .groupby("YearMonth")["amount_eur"].sum())
        months = sorted(set(list(invest_monthly.index) + list(real_monthly.index)))[-24:]
        fig, ax = plt.subplots(figsize=(10, 4))
        x = range(len(months))
        w = 0.4
        ax.bar([i - w/2 for i in x],
               [real_monthly.get(m, 0) for m in months],   w, label="Spese reali", color="#F44336", alpha=0.8)
        ax.bar([i + w/2 for i in x],
               [invest_monthly.get(m, 0) for m in months], w, label="Investimenti", color="#4CAF50", alpha=0.8)
        step = max(1, len(months) // 8)
        ax.set_xticks(range(0, len(months), step))
        ax.set_xticklabels([months[i] for i in range(0, len(months), step)],
                           rotation=45, ha="right", fontsize=8)
        ax.set_title("Spese Reali vs Investimenti Mensili (Ultimi 24 mesi)", fontweight="bold")
        ax.set_ylabel("EUR")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_invest_vs_spend = make_chart(fig)

    # ---- profile comparison chart ----
    chart_profiles = None
    if not df.empty:
        def profile_real_monthly(profile):
            pdf  = df[(df["profile"] == profile) & (df["type"] == "Expense")]
            mask = ~pdf.apply(analysis.is_investment_transfer, axis=1)
            return pdf[mask].groupby("YearMonth")["amount_eur"].sum()

        anna_exp = profile_real_monthly("Anna")
        fede_exp = profile_real_monthly("Federico")
        months   = sorted(set(list(anna_exp.index) + list(fede_exp.index)))[-24:]
        fig, ax  = plt.subplots(figsize=(10, 4))
        ax.plot(months, [anna_exp.get(m, 0) for m in months], marker="o", label="Anna",     color="#E91E63")
        ax.plot(months, [fede_exp.get(m, 0) for m in months], marker="s", label="Federico", color="#2196F3")
        step = max(1, len(months) // 8)
        ax.set_xticks(range(0, len(months), step))
        ax.set_xticklabels([months[i] for i in range(0, len(months), step)],
                           rotation=45, ha="right", fontsize=8)
        ax.set_title("Spese Reali Mensili per Profilo (Ultimi 24 mesi)", fontweight="bold")
        ax.set_ylabel("EUR")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_profiles = make_chart(fig)

    # ---- inflation chart ----
    chart_inflation = None
    if not df.empty:
        mask     = ~df.apply(analysis.is_investment_transfer, axis=1)
        expenses = df[(df["type"] == "Expense") & mask].copy()
        expenses = expenses[~expenses["category"].isin(analysis.EXCLUDE_FROM_INFLATION)]
        monthly  = expenses.groupby("YearMonth")["amount_eur"].sum().reset_index()
        monthly  = monthly.sort_values("YearMonth")
        monthly["YearMonth"] = pd.to_datetime(monthly["YearMonth"])
        monthly  = monthly.set_index("YearMonth")
        if len(monthly) >= 13:
            rolling   = monthly["amount_eur"].rolling(window=12).sum()
            inf_series = rolling.pct_change().dropna() * 100
            if not inf_series.empty:
                fig, ax = plt.subplots(figsize=(7, 4))
                ax.hist(inf_series, bins=20, alpha=0.7, color="#2196F3", edgecolor="black")
                ax.axvline(inf_series.quantile(0.60), color="red", linestyle="--",
                           label=f"60° percentile: {inf_series.quantile(0.60):.1f}%")
                ax.set_title("Distribuzione Inflazione Personale", fontweight="bold")
                ax.set_xlabel("Tasso di inflazione (%)")
                ax.set_ylabel("Frequenza")
                ax.legend()
                ax.grid(axis="y", alpha=0.3)
                fig.tight_layout()
                chart_inflation = make_chart(fig)

    # ---- monthly detail table (real expenses only) ----
    monthly_detail = []
    if not df.empty:
        mask    = ~df.apply(analysis.is_investment_transfer, axis=1)
        real_df = df[(df["type"] == "Expense") & mask]
        for ym in sorted(real_df["YearMonth"].unique(), reverse=True)[:24]:
            month_df = real_df[real_df["YearMonth"] == ym]
            cats     = month_df.groupby("category")["amount_eur"].sum().to_dict()
            monthly_detail.append({"month": ym, **cats})

    return render_template("reports.html",
        summary=full_summary,
        avg_income=avg_income,
        avg_expense=avg_expense,
        avg_savings=avg_savings,
        inflation=inflation,
        chart_cat=chart_cat,
        chart_invest_vs_spend=chart_invest_vs_spend,
        chart_inflation=chart_inflation,
        chart_profiles=chart_profiles,
        monthly_detail=monthly_detail,
        anomalous=anomalous,
    )


@app.route("/portfolio")
def portfolio():
    config = analysis.load_config()
    df     = analysis.get_transactions_df()

    fineco_invested    = analysis.compute_fineco_invested(df)
    conto_dep_invested = analysis.compute_conto_deposito_invested(df)

    port_data = analysis.get_portfolio_data(config)

    chart_allocation  = None
    chart_country     = None
    chart_currency    = None

    if port_data:
        port = pd.DataFrame(port_data["portfolio"])

        # ---- allocation pie ----
        if not port.empty:
            fig, ax = plt.subplots(figsize=(8, 6))
            colors = plt.cm.Set3(np.linspace(0, 1, len(port)))
            wedges, texts, autotexts = ax.pie(
                port["CurrentValue"],
                labels=port["Ticker"],
                autopct="%1.1f%%",
                startangle=90,
                colors=colors,
                textprops={"fontsize": 9},
            )
            ax.set_title("Allocazione Portafoglio per ETF", fontweight="bold")
            fig.tight_layout()
            chart_allocation = make_chart(fig)

        # ---- country bar chart ----
        if port_data["country_breakdown"]:
            cdf = pd.DataFrame(port_data["country_breakdown"]).head(15)
            fig, ax = plt.subplots(figsize=(8, 5))
            colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(cdf)))
            ax.barh(cdf["countries"], cdf["Percentage"], color=colors)
            ax.set_title("Esposizione Geografica (%)", fontweight="bold")
            ax.set_xlabel("%")
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))
            fig.tight_layout()
            chart_country = make_chart(fig)

        # ---- currency pie ----
        if port_data["currency_breakdown"]:
            curr_df = pd.DataFrame(port_data["currency_breakdown"])
            curr_df = curr_df[curr_df["Percentage"] > 0.5]
            if not curr_df.empty:
                fig, ax = plt.subplots(figsize=(6, 5))
                ax.pie(curr_df["Percentage"], labels=curr_df["currency"],
                       autopct="%1.1f%%", startangle=90)
                ax.set_title("Rischio Valuta", fontweight="bold")
                fig.tight_layout()
                chart_currency = make_chart(fig)

    # ---- investment trend (Fineco vs conto dep) ----
    chart_invest_trend = None
    if not df.empty:
        fineco_monthly = (df[
            (df["type"] == "Expense") &
            (df["category"] == "Finance") &
            (df["subcategory"] == "Fineco")
        ].groupby("YearMonth")["amount_eur"].sum().cumsum().reset_index(name="Fineco"))

        cd_monthly = (df[
            (df["type"] == "Expense") &
            (df["category"] == "Finance") &
            (df["subcategory"] == "Conto deposito")
        ].groupby("YearMonth")["amount_eur"].sum().cumsum().reset_index(name="ContoDeposito"))

        inv_trend = pd.merge(fineco_monthly, cd_monthly, on="YearMonth", how="outer").ffill().fillna(0)
        if not inv_trend.empty:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.stackplot(inv_trend["YearMonth"].astype(str),
                         inv_trend["Fineco"], inv_trend["ContoDeposito"],
                         labels=["Fineco (ETF)", "Conto Deposito"],
                         colors=["#4CAF50", "#2196F3"], alpha=0.85)
            step = max(1, len(inv_trend) // 8)
            ax.set_xticks(range(0, len(inv_trend), step))
            ax.set_xticklabels(inv_trend["YearMonth"].astype(str).iloc[::step],
                               rotation=45, ha="right", fontsize=8)
            ax.set_title("Capitale Investito Cumulativo", fontweight="bold")
            ax.set_ylabel("EUR")
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
            ax.legend()
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            chart_invest_trend = make_chart(fig)

    return render_template("portfolio.html",
        port_data=port_data,
        fineco_invested=round(fineco_invested, 2),
        conto_dep_invested=round(conto_dep_invested, 2),
        chart_allocation=chart_allocation,
        chart_country=chart_country,
        chart_currency=chart_currency,
        chart_invest_trend=chart_invest_trend,
    )


@app.route("/projections")
def projections():
    config = analysis.load_config()
    df     = analysis.get_transactions_df()
    params = config["params"]

    initial_liquidity    = params["liquidita_31_giu_22"]
    fineco_invested      = analysis.compute_fineco_invested(df)
    conto_dep_invested   = analysis.compute_conto_deposito_invested(df)
    total_invested       = fineco_invested + conto_dep_invested
    bank_balance         = analysis.savings_balance(df, initial_liquidity, config)
    current_liquidity    = bank_balance - total_invested

    # Try to get live portfolio value, fall back to invested capital
    port_data         = analysis.get_portfolio_data(config)
    investment_value  = port_data["total_value"] + conto_dep_invested if port_data else total_invested
    gross_return      = port_data["gross_return_pct"] if port_data else 0.0

    # Net assets = liquid cash + investments − deferred capital gains tax (26%)
    unrealised_gains  = max(investment_value - total_invested, 0)
    tax_on_gains      = unrealised_gains * 0.26
    current_net_assets = current_liquidity + investment_value - tax_on_gains

    proj    = analysis.FutureProjections(df, config)
    proj_df = proj.run(current_net_assets)

    chart_proj        = None
    chart_proj_detail = None

    if not proj_df.empty:
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(proj_df["Date"], proj_df["NetAssets"], color="#4CAF50", linewidth=2.5,
                label="Patrimonio Netto Proiettato")
        ax.fill_between(range(len(proj_df)), proj_df["NetAssets"], alpha=0.1, color="#4CAF50")
        step = max(1, len(proj_df) // 8)
        ax.set_xticks(range(0, len(proj_df), step))
        ax.set_xticklabels(proj_df["Date"].iloc[::step], rotation=45, ha="right", fontsize=9)
        ax.axhline(y=current_net_assets, color="blue", linestyle="--", alpha=0.5,
                   label=f"Attuale: €{current_net_assets:,.0f}")
        ax.set_title("Previsione Patrimonio Futuro (Spese reali, Finance escluso)", fontweight="bold")
        ax.set_ylabel("EUR")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        chart_proj = make_chart(fig)

        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        axes[0].plot(proj_df["Date"], proj_df["Income"],  color="#2196F3", label="Entrate")
        axes[0].plot(proj_df["Date"], proj_df["Expense"], color="#F44336", label="Spese reali")
        axes[0].set_xticks(range(0, len(proj_df), step))
        axes[0].set_xticklabels(proj_df["Date"].iloc[::step], rotation=45, ha="right", fontsize=8)
        axes[0].set_title("Entrate e Spese Reali Proiettate", fontweight="bold")
        axes[0].set_ylabel("EUR")
        axes[0].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        axes[0].legend(); axes[0].grid(axis="y", alpha=0.3)

        axes[1].bar(proj_df["Date"], proj_df["Savings"],
                    color=["#4CAF50" if s >= 0 else "#F44336" for s in proj_df["Savings"]])
        axes[1].set_xticks(range(0, len(proj_df), step))
        axes[1].set_xticklabels(proj_df["Date"].iloc[::step], rotation=45, ha="right", fontsize=8)
        axes[1].set_title("Risparmio Mensile Proiettato", fontweight="bold")
        axes[1].set_ylabel("EUR")
        axes[1].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"€{x:,.0f}"))
        axes[1].axhline(y=0, color="black", linewidth=0.8)
        axes[1].grid(axis="y", alpha=0.3)
        fig.tight_layout(pad=3)
        chart_proj_detail = make_chart(fig)

    proj_summary = []
    if not proj_df.empty:
        proj_df_c = proj_df.copy()
        proj_df_c["Year"] = proj_df_c["Date"].str[:4]
        yearly = proj_df_c.groupby("Year").agg(
            Total_Income  =("Income",    "sum"),
            Total_Expense =("Expense",   "sum"),
            Total_Savings =("Savings",   "sum"),
            End_NetAssets =("NetAssets", "last"),
        ).reset_index()
        proj_summary = yearly.to_dict("records")

    return render_template("projections.html",
        chart_proj=chart_proj,
        chart_proj_detail=chart_proj_detail,
        proj_summary=proj_summary,
        current_net_assets=round(current_net_assets, 2),
        current_liquidity=round(current_liquidity, 2),
        investment_value=round(investment_value, 2),
        gross_return=gross_return,
        fineco_invested=round(fineco_invested, 2),
        conto_dep_invested=round(conto_dep_invested, 2),
        params=params,
        config=config,
    )


@app.route("/api/subcategories/<category>")
def api_subcategories(category):
    from flask import jsonify
    return jsonify(SUBCATEGORIES.get(category, [""]))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
