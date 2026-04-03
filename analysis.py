"""
Financial analysis module.

Key financial concept:
  - Finance/Fineco and Finance/Conto deposito transactions are logged as "Expense"
    in MoneyCoach, but are actually investment transfers (not real consumption expenses).
  - For cash-flow / liquidity tracking: all expenses (including Finance) reduce the
    bank account balance.
  - For reporting averages and inflation: Finance is excluded so we measure real spending.
  - Net worth = current_liquidity + investment_market_value - unrealised_gains_tax(26%)
    where current_liquidity = bank_savings - fineco_invested - conto_deposito_invested
"""

import pandas as pd
import numpy as np
import json
import os
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "Finanze", "config.json")

INVESTMENT_CATEGORIES = {"Finance"}          # categories that are investment transfers
INVESTMENT_SUBCATS    = {"Fineco", "Conto deposito"}  # subcategories that are investments
EXCLUDE_FROM_INFLATION = {"Bimbo", "Housing", "Finance"}  # exclude from inflation calc


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def get_transactions_df(profile=None):
    if profile:
        rows = db.query(
            "SELECT * FROM transactions WHERE profile = %s ORDER BY date",
            [profile]
        )
    else:
        rows = db.query("SELECT * FROM transactions ORDER BY date")
    if not rows:
        return pd.DataFrame(columns=[
            "id", "date", "profile", "category", "subcategory",
            "amount", "currency", "amount_eur", "type", "notes"
        ])
    df = pd.DataFrame([dict(r) for r in rows])
    df["date"]       = pd.to_datetime(df["date"])
    df["amount"]     = df["amount"].astype(float)
    df["amount_eur"] = df["amount_eur"].astype(float)
    df["YearMonth"]  = df["date"].dt.to_period("M").astype(str)
    return df


def is_investment_transfer(row):
    """Return True if this expense is actually an investment transfer, not real spending."""
    sub = row.get("subcategory") if isinstance(row, dict) else row["subcategory"]
    return (
        row["type"] == "Expense"
        and row["category"] in INVESTMENT_CATEGORIES
        and str(sub) in INVESTMENT_SUBCATS
    )


def split_real_vs_investment(df):
    """Split expense rows into real expenses and investment transfers."""
    mask = df.apply(is_investment_transfer, axis=1)
    return df[~mask], df[mask]


# ---------------------------------------------------------------------------
# Monthly aggregates
# ---------------------------------------------------------------------------

def monthly_summary(df, exclude_investments=True):
    """
    Monthly income / real-expense / savings summary.
    
    When exclude_investments=True (default) Finance transfers are excluded from
    expenses so the 'Expense' figure reflects actual consumption.
    The 'Savings' column is income minus real expenses (not including invest. transfers).
    """
    if df.empty:
        return pd.DataFrame()

    income_df = df[df["type"] == "Income"]

    if exclude_investments:
        mask = ~df.apply(is_investment_transfer, axis=1)
        expense_df = df[(df["type"] == "Expense") & mask]
    else:
        expense_df = df[df["type"] == "Expense"]

    income  = income_df.groupby("YearMonth")["amount_eur"].sum().rename("Income")
    expense = expense_df.groupby("YearMonth")["amount_eur"].sum().rename("Expense")
    invest  = (df[df.apply(is_investment_transfer, axis=1)]
               .groupby("YearMonth")["amount_eur"].sum()
               .rename("Invested"))

    merged = pd.concat([income, expense, invest], axis=1).fillna(0)
    merged["Savings"] = merged["Income"] - merged["Expense"]
    return merged.reset_index().sort_values("YearMonth", ascending=False)


def category_breakdown(df, month=None, exclude_investments=True):
    mask = ~df.apply(is_investment_transfer, axis=1) if exclude_investments else pd.Series(True, index=df.index)
    data = df[(df["type"] == "Expense") & mask]
    if month:
        data = data[data["YearMonth"] == month]
    if data.empty:
        return pd.DataFrame()
    return data.groupby("category")["amount_eur"].sum().reset_index().sort_values("amount_eur", ascending=False)


def last_n_months_avg(df, n=12):
    """Average income / real-expense / savings over the last n months (Finance excluded)."""
    if df.empty:
        return 0, 0, 0
    summary = monthly_summary(df, exclude_investments=True)
    summary = summary.sort_values("YearMonth", ascending=False).head(n)
    return (
        round(float(summary["Income"].mean()),  2),
        round(float(summary["Expense"].mean()), 2),
        round(float(summary["Savings"].mean()), 2)
    )


# ---------------------------------------------------------------------------
# Liquidity / net worth
# ---------------------------------------------------------------------------

def compute_fineco_invested(df):
    """Total capital transferred to Fineco (investment account) — cumulative."""
    mask = (
        (df["type"] == "Expense") &
        (df["category"] == "Finance") &
        (df["subcategory"] == "Fineco")
    )
    return float(df[mask]["amount_eur"].sum())


def compute_conto_deposito_invested(df):
    """Total capital placed in the deposit account."""
    mask = (
        (df["type"] == "Expense") &
        (df["category"] == "Finance") &
        (df["subcategory"] == "Conto deposito")
    )
    return float(df[mask]["amount_eur"].sum())


def savings_balance(df, initial_liquidity, config):
    """
    Compute total bank-account balance (= initial balance + all cash flows).
    Finance transfers ARE included here because the money really left the bank.
    Only transactions after config data_giu are used (same as original analisi_liquidita).
    """
    if df.empty:
        return initial_liquidity
    cutoff = pd.to_datetime(config["params"]["data_giu"])
    post  = df[df["date"] >= cutoff].copy()
    income  = float(post[post["type"] == "Income"]["amount_eur"].sum())
    expense = float(post[post["type"] == "Expense"]["amount_eur"].sum())
    return initial_liquidity + income - expense


def savings_trend(df, initial_liquidity, config):
    """
    Monthly cumulative bank balance series.
    Finance transfers are included as outflows (money really left the bank).
    """
    if df.empty:
        return pd.DataFrame()

    cutoff = pd.to_datetime(config["params"]["data_giu"])
    post   = df[df["date"] >= cutoff].copy()

    monthly_all = (post.groupby("YearMonth").apply(
        lambda g: (
            g.loc[g["type"] == "Income", "amount_eur"].sum()
            - g.loc[g["type"] == "Expense", "amount_eur"].sum()
        )
    ).reset_index(name="NetCashflow"))

    monthly_all = monthly_all.sort_values("YearMonth")
    monthly_all["CumulativeSavings"] = monthly_all["NetCashflow"].cumsum() + initial_liquidity

    invested_monthly = (df[df.apply(is_investment_transfer, axis=1)]
                        .groupby("YearMonth")["amount_eur"].sum()
                        .cumsum().reset_index(name="CumInvested"))
    monthly_all = pd.merge(monthly_all, invested_monthly, on="YearMonth", how="left")
    monthly_all["CumInvested"] = monthly_all["CumInvested"].fillna(0)

    return monthly_all


def get_anomalous_months(config):
    return [item["date"][:7] for item in config.get("mesi_entrate_anomale", [])]


# ---------------------------------------------------------------------------
# Inflation (personal, based on real spending only)
# ---------------------------------------------------------------------------

def calculate_personal_inflation(df):
    if df.empty:
        return 0.0
    mask = ~df.apply(is_investment_transfer, axis=1)
    expenses = df[(df["type"] == "Expense") & mask].copy()
    expenses = expenses[~expenses["category"].isin(EXCLUDE_FROM_INFLATION)]
    monthly = expenses.groupby("YearMonth")["amount_eur"].sum().reset_index()
    monthly = monthly.sort_values("YearMonth")
    monthly["YearMonth"] = pd.to_datetime(monthly["YearMonth"])
    monthly = monthly.set_index("YearMonth")
    if len(monthly) < 13:
        return 0.0
    rolling    = monthly["amount_eur"].rolling(window=12).sum()
    inflation  = rolling.pct_change().dropna() * 100
    return round(float(inflation.quantile(0.60)), 2)


# ---------------------------------------------------------------------------
# Portfolio analysis
# ---------------------------------------------------------------------------

def get_portfolio_data(config):
    """
    Load ETF holdings files and fetch live prices from Yahoo Finance.
    Returns a dict with portfolio metrics or None on error.
    """
    import yfinance as yf
    from yahooquery import Ticker as YQ

    data_dir = os.path.join(os.path.dirname(__file__), "Finanze", "data", "etf")

    try:
        # ---- load holdings files ----
        anna_xlsx = os.path.join(data_dir, "file_titoli_anna.xlsx")
        anna_csv  = os.path.join(data_dir, "file_titoli_anna.csv")
        fede_xlsx = os.path.join(data_dir, "file_titoli_federico.xlsx")
        fede_csv  = os.path.join(data_dir, "file_titoli_federico.csv")

        dfs = []
        for xlsx, csv_path, name in [
            (anna_xlsx, anna_csv, "Anna"),
            (fede_xlsx, fede_csv, "Federico"),
        ]:
            if os.path.exists(xlsx):
                df = pd.read_excel(xlsx, header=5)
            elif os.path.exists(csv_path):
                df = pd.read_csv(csv_path, header=5)
            else:
                continue
            df["Profile"] = name
            df = df.rename(columns={"Isin": "ISIN"})
            dfs.append(df)

        if not dfs:
            return None
        holdings = pd.concat(dfs, ignore_index=True)

        # Clean: keep only rows with a valid ISIN and numeric Quantita
        required_cols = {"ISIN", "Quantita", "Prezzo"}
        missing = required_cols - set(holdings.columns)
        if missing:
            logging.warning(f"Missing columns in holdings files: {missing}")
            return None

        holdings = holdings.dropna(subset=["ISIN", "Quantita"])
        holdings["Quantita"] = pd.to_numeric(holdings["Quantita"], errors="coerce")
        holdings["Prezzo"]   = pd.to_numeric(holdings["Prezzo"],   errors="coerce")
        holdings = holdings.dropna(subset=["Quantita"])
        holdings = holdings[holdings["Quantita"] != 0]

        # Merge config ETF info (Ticker, TER)
        etf_config = pd.DataFrame(config["portfolio"]["etf"])  # [{Ticker, TER, ISIN}]
        holdings = holdings.merge(etf_config[["ISIN", "Ticker", "TER"]], on="ISIN", how="left")
        holdings = holdings.dropna(subset=["Ticker"])

        # ---- group by ticker ----
        portfolio = (holdings.groupby(["Ticker", "TER"])
                     .agg(Quantity=("Quantita", "sum"),
                          AvgPrice=("Prezzo", "mean"))
                     .reset_index())

        # ---- fetch live prices ----
        tickers_str = " ".join(portfolio["Ticker"].tolist())
        live = {}
        try:
            data = yf.download(tickers_str, period="2d", auto_adjust=True, progress=False)
            if not data.empty:
                close = data["Close"] if "Close" in data.columns else data
                for t in portfolio["Ticker"]:
                    if t in close.columns:
                        price = float(close[t].dropna().iloc[-1])
                        live[t] = price
        except Exception as e:
            logging.warning(f"yfinance download failed: {e}")

        # Fallback to yahooquery if needed
        missing_tickers = [t for t in portfolio["Ticker"] if t not in live]
        if missing_tickers:
            try:
                yq = YQ(" ".join(missing_tickers))
                prices = yq.price
                for t in missing_tickers:
                    if t in prices and "regularMarketPrice" in prices[t]:
                        live[t] = prices[t]["regularMarketPrice"]
            except Exception as e:
                logging.warning(f"yahooquery fallback failed: {e}")

        portfolio["CurrentPrice"]    = portfolio["Ticker"].map(live).fillna(portfolio["AvgPrice"])
        portfolio["CurrentValue"]    = portfolio["Quantity"] * portfolio["CurrentPrice"]
        portfolio["PurchaseValue"]   = portfolio["Quantity"] * portfolio["AvgPrice"]
        portfolio["Return"]          = portfolio["CurrentValue"] - portfolio["PurchaseValue"]
        portfolio["ReturnPct"]       = (portfolio["Return"] / portfolio["PurchaseValue"].replace(0, np.nan)) * 100

        total_invested = float(portfolio["PurchaseValue"].sum())
        total_value    = float(portfolio["CurrentValue"].sum())
        total_return   = total_value - total_invested
        gross_return   = round((total_return / total_invested * 100) if total_invested > 0 else 0, 2)

        # ---- allocation by ticker ----
        portfolio_list = portfolio.to_dict("records")
        for p in portfolio_list:
            for k, v in p.items():
                if isinstance(v, float) and np.isnan(v):
                    p[k] = 0.0
                elif hasattr(v, "item"):
                    p[k] = v.item()

        # ---- country breakdown from static file ----
        countries_path = os.path.join(data_dir, "countries.csv")
        country_breakdown = []
        if os.path.exists(countries_path):
            countries = pd.read_csv(countries_path, sep=";")
            countries["percentage"] = pd.to_numeric(countries["percentage"].astype(str).str.strip(), errors="coerce") / 100
            countries = countries.dropna(subset=["percentage"])
            merged_c = pd.merge(portfolio[["Ticker", "CurrentValue"]], countries,
                                left_on="Ticker", right_on="symbol", how="inner")
            merged_c["Exposure"] = merged_c["CurrentValue"] * merged_c["percentage"]
            risk = merged_c.groupby("countries")["Exposure"].sum().reset_index()
            risk["Percentage"] = risk["Exposure"] / total_value * 100
            risk = risk.sort_values("Percentage", ascending=False).head(20)
            country_breakdown = risk.to_dict("records")

        # ---- currency breakdown from static file ----
        currency_path = os.path.join(data_dir, "currency.csv")
        currency_breakdown = []
        if os.path.exists(currency_path):
            currency = pd.read_csv(currency_path, sep=";")
            # currency.csv has: symbol, countries, percentage, currency
            # merge countries + currency to get ticker → currency mapping
            if os.path.exists(countries_path):
                countries_df = pd.read_csv(countries_path, sep=";")
                countries_df["percentage"] = pd.to_numeric(
                    countries_df["percentage"].astype(str).str.strip(), errors="coerce") / 100
                curr_map = pd.merge(countries_df, currency, left_on="countries", right_on="countries", how="left")
                curr_map = curr_map.dropna(subset=["currency_y" if "currency_y" in curr_map.columns else "currency"])
                curr_col = "currency_y" if "currency_y" in curr_map.columns else "currency"
                curr_map = curr_map.rename(columns={curr_col: "currency"})
                merged_cur = pd.merge(portfolio[["Ticker", "CurrentValue"]],
                                      curr_map[["symbol", "percentage", "currency"]],
                                      left_on="Ticker", right_on="symbol", how="inner")
                merged_cur["Exposure"] = merged_cur["CurrentValue"] * merged_cur["percentage"]
                curr_risk = merged_cur.groupby("currency")["Exposure"].sum().reset_index()
                curr_risk["Percentage"] = curr_risk["Exposure"] / total_value * 100
                curr_risk = curr_risk.sort_values("Percentage", ascending=False)
                currency_breakdown = curr_risk.to_dict("records")

        return {
            "portfolio":          portfolio_list,
            "total_invested":     round(total_invested, 2),
            "total_value":        round(total_value, 2),
            "total_return":       round(total_return, 2),
            "gross_return_pct":   gross_return,
            "num_securities":     len(portfolio),
            "country_breakdown":  country_breakdown,
            "currency_breakdown": currency_breakdown,
        }

    except Exception as e:
        logging.error(f"Portfolio analysis failed: {e}", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Projections
# ---------------------------------------------------------------------------

class FutureProjections:
    def __init__(self, df, config):
        self.config    = config
        self.params    = config["params"]
        self.df        = df
        self.anomalous = get_anomalous_months(config)

    def _clean_income(self):
        income = self.df[self.df["type"] == "Income"].groupby("YearMonth")["amount_eur"].sum()
        income.index = pd.to_datetime(income.index)
        filtered = income[~income.index.strftime("%Y-%m").isin(self.anomalous)]
        return filtered

    def _clean_real_expenses(self):
        """Real expenses = all expenses MINUS Finance investment transfers."""
        mask = ~self.df.apply(is_investment_transfer, axis=1)
        exp  = self.df[(self.df["type"] == "Expense") & mask]
        exp_monthly = exp.groupby("YearMonth")["amount_eur"].sum()
        exp_monthly.index = pd.to_datetime(exp_monthly.index)
        return exp_monthly

    def run(self, current_net_assets):
        params = self.params
        clean_income   = self._clean_income()
        clean_expenses = self._clean_real_expenses()

        mean_income   = float(clean_income.tail(12).mean()) if len(clean_income)   >= 12 else float(clean_income.mean())
        mean_expenses = float(clean_expenses.tail(12).mean()) if len(clean_expenses) >= 12 else float(clean_expenses.mean())

        inflation_rate   = calculate_personal_inflation(self.df) / 100.0
        annual_return    = params.get("interesse_lordo_Portafoglio_azionario", 4) / 100.0
        annual_investment = params.get("investimento_annuale_ribilanciamento_stimato", 6000)
        rebalance_month  = str(params.get("mese_ribilanciamento", "04")).zfill(2)
        years            = params.get("anni_per_la_previsione", 11)

        max_date = pd.to_datetime(self.df["date"].max()) if not self.df.empty else pd.Timestamp.now()
        rows = []
        liquidity        = current_net_assets
        investment_value = 0.0
        monthly_return   = (1 + annual_return) ** (1 / 12) - 1

        for i in range(years * 12):
            proj_date = max_date + relativedelta(months=i + 1)
            month_str = proj_date.strftime("%Y-%m")

            projected_income  = mean_income
            projected_expense = mean_expenses * ((1 + inflation_rate) ** (i / 12))

            corrections = _config_income_corrections(self.config, month_str)
            if corrections:
                projected_income = corrections

            if proj_date.strftime("%m") == rebalance_month:
                investment_value += annual_investment
                liquidity        -= annual_investment

            investment_value *= (1 + monthly_return)

            monthly_savings = projected_income - projected_expense
            liquidity      += monthly_savings

            net_assets = liquidity + investment_value * 0.74

            rows.append({
                "Date":       month_str,
                "Income":     round(projected_income,  2),
                "Expense":    round(projected_expense, 2),
                "Savings":    round(monthly_savings,   2),
                "Investment": round(investment_value,  2),
                "NetAssets":  round(net_assets,        2),
            })

        return pd.DataFrame(rows)


def _config_income_corrections(config, month_str):
    corrections = config.get("correzioni", {}).get("reddito", {})
    month_key   = month_str + "-01"
    return corrections.get(month_key, None)
