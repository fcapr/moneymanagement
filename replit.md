# Finanze - Personal Finance Tracker

## Overview
A Flask web application for tracking personal finances for two profiles (Federico and Anna). Built on top of the original Finanze Python analysis scripts, with data stored in a PostgreSQL database. Includes automated expense reporting, savings trends, personal inflation calculation, and future net worth projections.

## Architecture
- **Backend**: Flask (Python), PostgreSQL via psycopg2
- **Frontend**: Bootstrap 5, server-rendered HTML, Matplotlib for charts (embedded as base64 PNG)
- **Database**: Replit PostgreSQL (DATABASE_URL env var)
- **Live prices**: Yahoo Finance via `yfinance` and `yahooquery`

## Key Files
- `app.py` — Main Flask application with all routes
- `analysis.py` — Financial analysis logic (monthly summaries, projections, inflation, portfolio)
- `db.py` — Database connection utilities
- `import_data.py` — One-time migration script (CSV → PostgreSQL)
- `templates/` — Jinja2 HTML templates (base, dashboard, transactions, add/edit, reports, portfolio, projections)
- `Finanze/config.json` — Financial projection parameters + ETF config (portfolio.etf)
- `Finanze/data/etf/` — ETF holdings files (file_titoli_*.xlsx), countries.csv, currency.csv

## Critical Financial Logic

### Investment Transfers vs Real Expenses
- `Finance/Fineco` and `Finance/Conto deposito` are logged as "Expense" in MoneyCoach **but are actually investment transfers**, not consumption spending
- All expense averages, inflation calculations, and monthly summaries **EXCLUDE** these categories
- For cash-flow tracking (bank balance), Finance transfers are included as outflows (money really left the bank)

### Net Worth Formula
```
bank_balance      = initial_liquidity + cumsum(all_income - all_expenses_including_finance)
fineco_invested   = cumulative Finance/Fineco outflows
conto_dep         = cumulative Finance/Conto deposito outflows
current_liquidity = bank_balance - fineco_invested - conto_dep
investment_value  = ETF market value (live) + conto deposito
net_assets        = current_liquidity + investment_value - max((investment_value - invested_capital) * 0.26, 0)
```
The 0.26 factor is the Italian capital gains tax on unrealised gains.

### Inflation Calculation
Personal inflation excludes Finance, Housing, and Bimbo categories (to measure pure consumption inflation).

## Features
1. **Dashboard** — Liquidity breakdown (bank balance vs real liquid cash vs invested), averages with Finance excluded, savings trend
2. **Transactions** — Add, edit, delete expenses/income per profile (Federico/Anna)
3. **Reports** — Category breakdown (Finance excluded), real vs investments chart, profile comparison, personal inflation distribution
4. **Portfolio** — Live ETF prices, invested capital tracking, allocation/country/currency breakdown from Fineco holding files
5. **Projections** — Future net worth (€152K → €300K over 11 years) based on real expense averages + config parameters

## Database Schema
```sql
transactions (id, date, profile, category, subcategory, amount, currency, amount_eur, type, notes)
portfolio_holdings (id, profile, ticker, isin, quantity, purchase_price, ter)
app_config (key, value)
```

## Running
The app runs on port 5000 via `python app.py`.

## Data Import
Run `python import_data.py` to import historical CSV data into the database (already done once — 7,151 transactions: Anna 1,395, Federico 5,756).

## Configuration (Finanze/config.json)
- `params.liquidita_31_giu_22`: Initial liquidity anchor (€115,610.91)
- `params.data_giu`: Reference date for savings calculation (2026-01-03)
- `params.mesi_di_spese_correnti`: Months of expenses to keep liquid (2)
- `params.mesi_di_spese_impreviste`: Months of expenses in deposit account (5)
- `params.interesse_lordo_Portafoglio_azionario`: Expected annual return % (4)
- `params.investimento_annuale_ribilanciamento_stimato`: Annual PAC investment (€6,000)
- `mesi_entrate_anomale`: List of months to exclude from income averages (job changes, maternity)
- `portfolio.etf`: List of ETFs with Ticker, TER, ISIN for portfolio analysis
