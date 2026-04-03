"""
Script to import existing CSV transaction data into the PostgreSQL database.
Run once to migrate historical data.
"""
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL")

def import_transactions():
    data_dir = os.path.join(os.path.dirname(__file__), "Finanze", "data")
    csv_files = {
        "Anna": os.path.join(data_dir, "transactions_Anna.csv"),
        "Federico": os.path.join(data_dir, "transactions_Federico.csv"),
    }

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    total = 0
    for profile, path in csv_files.items():
        if not os.path.exists(path):
            print(f"File not found, skipping: {path}")
            continue

        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]

        date_col = [c for c in df.columns if "Data" in c or "date" in c.lower()][0]
        amount_col = [c for c in df.columns if "Importo" in c and "EUR" not in c and "conv" not in c.lower()][0]
        eur_col = [c for c in df.columns if "EUR" in c][0] if any("EUR" in c for c in df.columns) else amount_col
        cat_col = [c for c in df.columns if "Categoria" in c and "Sotto" not in c][0]
        sub_col = [c for c in df.columns if "Sotto" in c][0] if any("Sotto" in c for c in df.columns) else None
        type_col = [c for c in df.columns if "Tipo" in c or "Type" in c.lower()][0]
        currency_col = [c for c in df.columns if "Valuta" in c or c.lower() == "currency"][0] if any("Valuta" in c for c in df.columns) else None

        rows = []
        skipped = 0
        for _, row in df.iterrows():
            try:
                date_val = pd.to_datetime(str(row[date_col])).date()
                cat = str(row[cat_col]).strip() if pd.notna(row[cat_col]) else "Other"
                sub = str(row[sub_col]).strip() if sub_col and pd.notna(row[sub_col]) else ""
                amount = float(row[amount_col]) if pd.notna(row[amount_col]) else 0
                amount_eur = float(row[eur_col]) if pd.notna(row[eur_col]) else amount
                currency = str(row[currency_col]).strip() if currency_col and pd.notna(row[currency_col]) else "EUR"
                tx_type = str(row[type_col]).strip()
                if tx_type not in ("Expense", "Income"):
                    skipped += 1
                    continue
                rows.append((date_val, profile, cat, sub, amount, currency, amount_eur, tx_type, ""))
            except Exception as e:
                skipped += 1
                continue

        if rows:
            execute_values(
                cur,
                """INSERT INTO transactions (date, profile, category, subcategory, amount, currency, amount_eur, type, notes)
                   VALUES %s ON CONFLICT DO NOTHING""",
                rows
            )
            print(f"  Imported {len(rows)} rows for {profile} (skipped {skipped})")
            total += len(rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone! Total imported: {total} transactions.")

if __name__ == "__main__":
    import_transactions()
