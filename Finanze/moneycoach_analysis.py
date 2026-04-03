# -*- coding: utf-8 -*-
import token_gmail
import utils
import pandas as pd
import datetime as dt
import numpy as np
from jinja2 import Environment, FileSystemLoader
import base64
import os
import json
import logging  # Import logging module
import matplotlib.pyplot as plt
from yahooquery import Ticker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FinancialAnalysis:
    def __init__(self):
        """Initialize the FinancialAnalysis class and load configuration."""
        config_path = "config.json"
        
        # # Stampa informazioni di debug
        # logging.info("Current working directory: %s", os.getcwd())
        # logging.info("Percorso completo del file: %s", os.path.abspath(config_path))
        
        try:
            # Verifica esistenza e permessi
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"File non trovato: {config_path}")
            if not os.access(config_path, os.R_OK):
                raise PermissionError(f"Permessi di lettura negati per: {config_path}")
            
            with open(config_path, "r", encoding='utf-8') as file:
                self.config = json.load(file)
        except Exception as e:
            raise RuntimeError(f"Errore durante il caricamento del file di configurazione: {e}")

        self.portfolio_config = self.config['portfolio']

        self.anomalous_months = self.config.get("mesi_entrate_anomale", [])
        self.anomalous_months = [item["date"] for item in self.anomalous_months if "date" in item]
        self.initial_liquidity = self.config['params']['liquidita_31_giu_22']
        self.desired_interest = self.config['params']["minimum_desired_annual_interest_rate_for_deposit_account_percent"]

        self.template_html = """
    <html>
    <head>
        <title>Update Mensile</title>
        <style>
        table {
            font-family: Arial, sans-serif;
            background-color: rgb(255, 247, 247);
            font-size: 12px;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        table th, table td {
            border: 1px solid black;
            padding: 5px;
            text-align: left;
        }
        body {
            font-family: Calibri, sans-serif;
            background-image: url(background.jpg);
        }
        h1 {
            color: rgb(0, 0, 153);
        }
        </style>
    </head>
    <body>
        <h1>Update Mensile</h1>
        Salve,<br />
        Di seguito l'aggiornamento patrimoniale mensile.<br />
        Risparmiato fino ad ora:
        <th>{{ risp_fino_ad_ora }}</th>
        euro.<br />
        Patrimonio attuale netto:
        <th>{{ patrimonio_ad_oggi }}</th>
        euro.<br />
        Rendimento lordo investimenti:
        <th>{{ Rendimento_attuale_fineco_lordo }}</th>
        %.<br />
        Grazie,<br />
        Federico<br />
        <br />

        <h4>Recap Mensile:</h4>

        <table>
        <tr>
            <th>YearMonth</th>
            {% for column in columns_1 %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            <th>Entrate</th>
            {% for row in rows_1 %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            {% endfor %}
        </tr>
            <th>Uscite</th>
            {% for row in rows_2 %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            {% endfor %}
        </tr>
            <th>Risparmiato</th>
            {% for row in rows_3 %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            {% endfor %}
        </table>

        <h4>Details:</h4>

        <table>
        <tr>
            {% for column in columns_detail %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_detail %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>
        <br />

        <h4>Storico e Ultimo mese</h4>
        <br />
        <img src="data:image/jpg;base64,{{ image_data_hist }}" alt="Line Plot" width="600" height="430">
        <br /><br />

        <h4>Kpi Base:</h4>
        <table>
        <tr>
            <th>Spesa media</th>
            <td>{{ last_18_months_mean_spese }}</td>
        </tr>
        <tr>
            <th>Reddito medio</th>
            <td>{{ last_18_months_mean_reddito }}</td>
        </tr>
        <tr>
            <th>Risparmio medio</th>
            <td>{{ last_18_months_mean_saving }}</td>
        </tr>
        </table>

        <br /><br />
        <h4>Valori secondari:</h4>
        <table>
        <tr>
            <th>Reddito medio Fede</th>
            <td>{{ last_12_months_mean_Fede }}</td>
        </tr>
        <tr>
            <th>Reddito medio Anna</th>
            <td>{{ last_12_months_mean_Anna }}</td>
        </tr>
        <tr>
            <th>Bonus vari Giulio</th>
            <td>{{ Valore_medio_assegno_unico }}</td>
        </tr>
        <tr>
            <th>Spesa media Giulio</th>
            <td>{{ values_bimbo_mean }}</td>
        </tr>
        </table>

        <br /><br />
        <h4>Investimenti:</h4>
        <p>{{ xeon_message }}</p>
        <table>
        <tr>
            <th>Capitale investito</th>
            <td>{{ capitale_investito_Fineco }}</td>
        </tr>
        <tr>
            <th>Trend attuale %</th>
            <td>{{ Rendimento_attuale_fineco_lordo }}</td>
        </tr>
        <tr>
            <th>Numero prodotti</th>
            <td>{{ num_securities_held }}</td>
        </tr>
        <tr>
            <th>Controvalore investimento</th>
            <td>{{ controvalore_investimento }}</td>
        </tr>
        <tr>
            <th>Liquidità attuale</th>
            <td>{{ Liquidita_attuale }}</td>
        </tr>
        <tr>
            <th>Liquidità ideale</th>
            <td>{{ Liquidita_ideale }}</td>
        </tr>
        <tr>
            <th>Conto deposito attuale</th>
            <td>{{ Conto_deposito_attuale }}</td>
        </tr>
        <tr>
            <th>Conto deposito ideale</th>
            <td>{{ Conto_deposito_ideale }}</td>
        </tr>
        <tr>
            <th>Importo che si potrebbe investire</th>
            <td>{{ Importo_da_investire }}</td>
        </tr>
        </table>

        <h4>Drill down portafoglio:</h4>
        <br />
        <img src="data:image/jpg;base64,{{ Trend_investimenti }}" width="900">
        <br /><br />

        <h4>Allocazione Patrimonio:</h4>
        <br />
        <img src="data:image/jpg;base64,{{ allocazone_portafoglio }}" width="700">
        <br /><br />


        <em>Rischio valuta:</em>

        <table>
        <tr>
            {% for column in columns_currency_risk %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_currency_risk %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <em>Rischio paese:</em>

        <table>
        <tr>
            {% for column in columns_country_risk  %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_country_risk  %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <em>Peso settori:</em>

        <table>
        <tr>
            {% for column in columns_max_sector_new  %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_max_sector_new %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <em>Top partecipate:</em>

        <table>
        <tr>
            {% for column in columns_max_holdings_new  %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_max_holdings_new %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <br /><br />
        <h4>Condizioni sulla Previsione:</h4>
        Ipotizzando:<br />
        - <th>{{ anni_futuri }}</th> anni di previsione, <br />
        - il costo dell'asilo nido e, successivamente, della materna, <br />
        - il cambio lavoro di Anna nel <th>{{ data_cambio_lavoro_Anna }}</th>,<br />
        - il cambio casa nel <th>{{ data_cambio_casa }}</th>,<br />
        - i viaggi con un costo maggiorato del 30%,<br />
        - il viaggio in America nel <th>{{ data_viaggio_america }}</th>,<br />
        - un investimento ad aprile una volta all'anno nel PAC di Fineco di <th>{{ investimento_annuale_ribilanciamento_stimato }}</th> euro,<br />
        - ritorno dell'investimento lordo stimato del <th>{{interesse_lordo_Portafoglio_azionario}}</th>%,<br />
        <br />

        <h4>Previsioni Future:</h4>

        <table>
        <tr>
            {% for column in columns_future %}
            <th>{{ column }}</th>
            {% endfor %}
        </tr>
            {% for row in rows_future %}
            {% for item in row %}
            <td>{{ item }}</td>
            {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <br /><br />

    </body>
    </html>
    """

        self.merged = None

    def read_and_process_data(self):
        """Read and process the latest transaction data."""

        # Trova gli ultimi due file ZIP che iniziano con "Backup_"
        backup_files = sorted(
            [f for f in os.listdir("data") if f.startswith("Backup_") and f.endswith(".zip")],
            reverse=True  # Ordina per nome (assumendo che la data sia parte del nome)
        )[:2]  # Prendi i due file più recenti

        # Log dei file trovati
        logging.info(f"Found backup files: {backup_files}")

        # Esegui lo script per ciascuno di essi
        for backup in backup_files:
            utils.process_backup(os.path.join("data", backup))  # Assicurati di passare il percorso corretto

        try:
            latest_files = utils.read_latest_files()
            self.data = utils.transform_dates(latest_files)
        except Exception as e:
            logging.error("Error reading or processing data: %s", e)
            raise

        self.fineco_investment = self.data[
            (self.data['Tipo']=="Expense") & 
            (self.data['Categoria']=="Finance") &
            (self.data['SottoCategoria']=="Fineco")
        ]

        self.conto_deposito = self.data[
            (self.data['Tipo']=="Expense") & 
            (self.data['Categoria']=="Finance") &
            (self.data['SottoCategoria']=="Conto deposito")
        ]
        
        self.all_data = utils.transform_transactions(self.data)
        self.time_series = self.all_data[['ds','y']]
        utils.time_series_graph(self.time_series, self.config)
        
        self.all_data['YearMonth'] = self.all_data['ds'].apply(lambda x: str(x)[:7])

    def create_pivot_tables(self):
        """Create pivot tables for expenses and income."""
        self.expense_table = pd.pivot_table(
            self.all_data[self.all_data['Tipo']=='Expense'],
            values='Importo',
            index='YearMonth', 
            columns=['Categoria'],
            aggfunc=np.sum
        )
        
        self.income_pivot = pd.pivot_table(
            self.all_data[self.all_data['Tipo']=='Income'],
            values='Importo',
            columns=['YearMonth'],
            aggfunc=np.sum
        )
        
        self.expense_pivot = pd.pivot_table(
            self.all_data[self.all_data['Tipo']=='Expense'],
            values='Importo',
            columns=['YearMonth'],
            aggfunc=np.sum
        )
        
        self.food_pivot = pd.pivot_table(
            self.all_data[self.all_data['Categoria']=='Food'],
            values='Importo',
            index=['Categoria','SottoCategoria'],
            columns=['YearMonth'],
            aggfunc=np.sum
        )

        logging.info("Pivot tables created successfully.")

    def process_tables(self):
        """Process the tables to generate savings, income, and expense data."""
        utils.bar_chart(self.all_data)
        self.allocation, self.income, self.expenses = utils.manipolazione_table(self.all_data)

        self.allocation = self.allocation.reset_index()

        # # Add debug log to check columns
        # logging.info("Columns in allocation: %s", self.allocation.columns.tolist())
        
        tmp = self.allocation.copy()
        
        # Check if YearMonth exists, if not, create it
        if 'YearMonth' not in tmp.columns:
            if 'ds' in tmp.columns:
                tmp['YearMonth'] = tmp['ds'].astype(str).str[:7]
            else:
                raise ValueError("Neither 'YearMonth' nor 'ds' column found in allocation DataFrame")
        
        tmp['YearMonth'] = tmp['YearMonth'].str.replace('-', '')
        
        self.savings_pivot = pd.pivot_table(tmp, values='Risparmiato', columns=['YearMonth'], aggfunc=np.sum)
        self.income_pivot = pd.pivot_table(tmp, values='Income', columns=['YearMonth'], aggfunc=np.sum)
        self.expense_pivot = pd.pivot_table(tmp, values='Expense', columns=['YearMonth'], aggfunc=np.sum)

        # Reorder columns in chronological order
        current_columns = list(self.savings_pivot.columns)
        desired_order = sorted(current_columns)  # Sort in ascending order
        
        # Directly assign the reindexed and reversed tables
        self.income_pivot = self.income_pivot.reindex(columns=desired_order).iloc[:, ::-1]
        self.expense_pivot = self.expense_pivot.reindex(columns=desired_order).iloc[:, ::-1]
        self.savings_pivot = self.savings_pivot.reindex(columns=desired_order).iloc[:, ::-1]

    def save_and_process_table(self):
        """Save and process the expense table for further analysis."""
        self.update = self.all_data['ds'].max()
        table_export = self.expense_table.rename_axis(None, axis=0).fillna(0)
        table_export.reset_index(drop=False).to_csv("file_table.csv")
        
        table = pd.read_csv('file_table.csv', sep=',')
        self.detail_table = (table.drop('Unnamed: 0', axis=1)
                           .rename(columns={'index':'YearMonth'})
                           .sort_values(by='YearMonth', ascending=False))
        
        # logging.info(self.detail_table.head(10))
    
    def calculate_inflation_rate(self):
        """Calculate the inflation rate based on historical spending data."""
        # Crea una copia della detail_table senza le colonne 'Bimbo' e 'Abitazione'
        filtered_table = self.detail_table.drop(columns=['Bimbo', 'Housing'], errors='ignore')
        
        # Calcola il totale delle spese per ogni riga, escludendo la colonna 'YearMonth'
        numeric_columns = filtered_table.select_dtypes(include=[np.number]).columns
        filtered_table['Totale'] = filtered_table[numeric_columns].sum(axis=1)
        
        # Estrai la serie storica con YearMonth e Totale
        historical_series = filtered_table[['YearMonth', 'Totale']]
        
        # Ordina la serie storica in base a YearMonth
        historical_series['YearMonth'] = pd.to_datetime(historical_series['YearMonth'])
        historical_series = historical_series.sort_values(by='YearMonth')
        
        # Imposta YearMonth come indice
        historical_series.set_index('YearMonth', inplace=True)
        
        # Calcola le finestre rolling di 12 mesi
        rolling_totals = historical_series['Totale'].rolling(window=12).sum()
        
        # Calcola il tasso di inflazione come variazione percentuale
        inflation_rate = rolling_totals.pct_change().dropna() * 100  # Moltiplica per 100 per ottenere la percentuale
        
        # # Stampa statistiche descrittive sulla varianza delle inflazioni calcolate
        # logging.info("Statistiche descrittive sull'inflazione:")
        # logging.info(inflation_rate.describe())
        
        # Crea un grafico per mostrare la distribuzione delle inflazioni
        plt.figure(figsize=(10, 6))
        plt.hist(inflation_rate, bins=30, alpha=0.7, color='blue', edgecolor='black')
        plt.title('Distribuzione delle Inflazioni',fontweight='bold')
        plt.xlabel('Tasso di Inflazione (%)')
        plt.ylabel('Frequenza')
        plt.grid(axis='y', alpha=0.75)
        plt.savefig('Inflation_rate.jpg', transparent=True)
        plt.close()

        logging.info("Inflazione storica aggiustata: %s", inflation_rate.quantile(0.60))

        self.inflation_rate = inflation_rate.quantile(0.60)

    def calculate_metrics(self):
        """Calculate various financial metrics based on income and expenses."""
        self.mean_income, self.mean_expenses, self.mean_savings = utils.aggiunta_info_recap(
            self.income, self.anomalous_months, self.expenses
        )
        
        expenses = self.all_data[self.all_data['Tipo']=='Expense']
        income = self.all_data[self.all_data['Tipo']=='Income']
        
        self.fede_mean, self.anna_mean, self.bonus_mean, self.child_expenses_mean = utils.dati_sintesi(
            income, expenses, self.anomalous_months, self.config
        )
        
        self.savings = utils.analisi_liquidita(self.all_data, self.initial_liquidity, self.config)
        
        self.fineco_invested, self.conto_deposito_invested, self.stock_price, self.investment_months = utils.investimenti_ad_ora(
            self.fineco_investment, self.conto_deposito, self.config
        )


        logging.info("Metrics calculated successfully.")

    def process_portfolio(self):
        """Process the investment portfolio and calculate various metrics."""
        current_dir = os.getcwd()
        df_anna = pd.read_excel(os.path.join(current_dir, "data/etf/file_titoli_anna.xlsx"), header=5)
        df_fede = pd.read_excel(os.path.join(current_dir, "data/etf/file_titoli_federico.xlsx"), header=5)

        df_anna = df_anna.rename(columns = {'Isin':'ISIN'})
        df_fede = df_fede.rename(columns = {'Isin':'ISIN'})

        countries = pd.read_csv(os.path.join(current_dir, "data/etf/countries.csv"), sep=';')
        currency = pd.read_csv(os.path.join(current_dir, "data/etf/currency.csv"), sep=';')

        df = pd.concat([df_anna, df_fede])
        
        # Add debug log to check the output of analyze_portfolio
        result = utils.analyze_portfolio(
            df, self.portfolio_config, countries, currency, self.stock_price, self.config
        )
        # Unpack the result
        (self.investment_value_portfolio, self.investment_deposit_account, self.num_securities, self.currency_risk, 
         self.country_risk, self.sector_weights, self.holdings_weights, self.portfolio_actual) = result
        
        self.current_liquidity = self.savings - self.fineco_invested - self.conto_deposito_invested
        self.ideal_liquidity = self.mean_expenses * self.config['params']['mesi_di_spese_correnti']
        self.deposit_account_ideal = self.mean_expenses * self.config['params']['mesi_di_spese_impreviste']
        self.investable_amount = max(self.current_liquidity - self.ideal_liquidity,0)

        self.investment_value = self.investment_value_portfolio + self.investment_deposit_account
        
        self.current_net_assets = self.current_liquidity + self.investment_value - max((self.investment_value - (self.fineco_invested + self.conto_deposito_invested)) * 0.26, 0)
        self.gross_return = round((self.investment_value_portfolio/self.fineco_invested-1)*100, 2)

        # Inizializza una lista vuota per contenere i dizionari delle righe del DataFrame
        portfolio_data = []

        # Itera su ogni ticker nel DataFrame
        for ticker in self.portfolio_actual['Ticker'].unique():
            # Ottieni il TER dal DataFrame df
            ter = self.portfolio_actual[self.portfolio_actual['Ticker'] == ticker]['TER'].iloc[0]
            
            # Ottieni il prezzo attuale del ticker
            current_price = Ticker(ticker).price[ticker]['regularMarketPrice']
            
            # Ottieni le quantità possedute per il ticker
            quantities = self.portfolio_actual[self.portfolio_actual['Ticker'] == ticker]['Quantita']
            
            # Calcola il controvalore moltiplicando la quantità posseduta per il valore attuale del titolo
            controvalore = quantities * current_price
            
            # Aggiungi una riga alla lista portfolio_data con le informazioni del ticker
            portfolio_data.append({
                'Ticker': ticker,
                'Ter': ter,
                'Quantita': quantities.sum(),  # Somma delle quantità possedute per il ticker
                'Valore Attuale': current_price,
                'Controvalore': controvalore.sum()
            })

        # Crea il DataFrame dal list di dizionari
        portfolio_current = pd.DataFrame(portfolio_data)

        # New code to calculate category holdings
        category_holdings_df_final = pd.DataFrame()
        ticker_symbols = portfolio_current['Ticker'].tolist()
        bond_list = []

        for symbol in ticker_symbols:
            # Fetch sector weightings for the current ticker
            t = Ticker(symbol)
            
            # Check if fund_category_holdings is a DataFrame or a dict
            category_holdings_df = t.fund_category_holdings
            
            # Convert to DataFrame if it's a dictionary
            if isinstance(category_holdings_df, dict):
                category_holdings_df = pd.DataFrame.from_dict(category_holdings_df)
            elif isinstance(category_holdings_df, pd.DataFrame):
                category_holdings_df = category_holdings_df
            else:
                logging.warning("Unexpected type for fund_category_holdings for ticker: %s", symbol)
                continue  # Skip to the next ticker if the type is unexpected

            category_holdings_df = category_holdings_df.reset_index()
            
            # Use pd.concat instead of append
            category_holdings_df_final = pd.concat([category_holdings_df_final, category_holdings_df], ignore_index=True)

        category_holdings_df_final = category_holdings_df_final.rename(columns={'index':'Ticker'})

        category_holdings_df_final['Bonds'] = category_holdings_df_final['bondPosition']
        category_holdings_df_final['Stocks'] = category_holdings_df_final['stockPosition']
        category_holdings_df_final['Others'] = 1 - (category_holdings_df_final['Bonds'] + category_holdings_df_final['Stocks'])

        category_holdings_df_final = category_holdings_df_final[['Ticker','Bonds','Stocks','Others']]

        category_holdings_df_final['Cash'] = 0
        category_holdings_df_final['ETF Monetario'] = 0

        category_holdings_df_final = pd.merge(category_holdings_df_final, portfolio_current, on='Ticker')

        category_holdings_df_final = category_holdings_df_final[['Ticker','Bonds','Stocks','Others','Cash','ETF Monetario','Controvalore']]

        # New row as a DataFrame
        new_row = pd.DataFrame({'Ticker': ['Cash','Conto Deposito'], 'Bonds': [0,0], 'Stocks': [0,0],'Others': [0,0], 'Cash': [1,0], 'ETF Monetario': [0,1], 'Controvalore': [self.current_liquidity,self.investment_deposit_account]})

        # Add the new row
        df = pd.concat([category_holdings_df_final, new_row], ignore_index=True)

        # Calculate total controvalore
        total_controvalore = df['Controvalore'].sum()

        # Calculate weighted percentages for each category
        categories = ['Bonds', 'Stocks', 'Others', 'Cash','ETF Monetario']
        weighted_percentages = {}

        for category in categories:
            weighted_percentages[category] = (df[category] * df['Controvalore']).sum() / total_controvalore * 100

        # Create a pie chart with enhanced visualization
        plt.figure(figsize=(10, 8))

        # Explode the largest slice slightly for emphasis
        explode = [0.1 if v == max(weighted_percentages.values()) else 0 for v in weighted_percentages.values()]

        # Plot the pie chart with startangle=90
        plt.pie(
            weighted_percentages.values(),
            labels=weighted_percentages.keys(),
            autopct='%1.1f%%',       # Show percentage with 1 decimal place
            startangle=90,           # Start the first slice at the top
            explode=explode,         # Explode the largest slice
            shadow=False,            # Add shadow for better visualization
            textprops={'fontsize': 12}  # Increase font size for labels
        )

        # Add a title
        plt.title('Portfolio Allocation', fontsize=14, fontweight='bold')

        # Display the chart
        plt.savefig('Portfolio_Allocation.jpg', transparent=True)
        plt.close()

    def prepare_merged_data(self):
        """Prepare merged data for future projections."""
        if not hasattr(self, 'income') or not hasattr(self, 'expenses'):
            raise ValueError("Income and expenses data not initialized")
        
        self.merged = pd.merge(self.income, self.expenses, on='YearMonth', how='outer')
        self.merged = self.merged.fillna(0)
        logging.info("Merged data prepared: %s", self.merged.shape)

    def calculate_rolling_means(self):
        """Calculate rolling means for expenses."""
        if self.merged is None:
            raise ValueError("Merged data not prepared")
        
        window_size = 12
        # Calcola la media mobile
        rolling_mean = (
            self.merged['Spesa_new']
            .rolling(window=window_size, min_periods=1)
            .mean()
            .shift(-window_size + 1)
        )
        
        # Assegna il valore senza usare fillna inplace
        self.merged['Rolling_Spesa_Mean'] = rolling_mean.fillna(rolling_mean.mean())

    def calculate_investment_projections(self):
        """Calculate investment projections."""
        # Calcola le proiezioni degli investimenti come nel vecchio codice
        listone = list(self.merged['YearMonth'].values)
        
        # Inizializza DataFrame per proiezioni investimenti
        self.investment_projections = pd.DataFrame(columns=['YearMonth', 'Valore_investimento_Fineco', 'Capitale_Investito'])
        
        # Calcola interesse mensile
        interesse_annuo = self.config['params']['interesse_lordo_Portafoglio_azionario'] / 100
        calcolo_interesse_mensile = (1 + interesse_annuo) ** (1/12) - 1
        
        # Valori iniziali
        if self.investment_months > 0:
            initial_value = (
                self.investment_value_portfolio * (1 + calcolo_interesse_mensile) + 
                self.config['params']['importo_PAC_iniziale']
            )
        else:
            initial_value = self.investment_value_portfolio * (1 + calcolo_interesse_mensile)

            
        # Prima riga
        self.investment_projections.loc[0] = [
            listone[0], 
            initial_value,
            self.fineco_invested
        ]
        
        # Calcola valori futuri
        for i, date in enumerate(listone[1:], 1):
            prev_value = self.investment_projections.loc[i-1, 'Valore_investimento_Fineco']
            prev_capital = self.investment_projections.loc[i-1, 'Capitale_Investito']
            
            month = pd.to_datetime(date).strftime('%m')
            
            base_value = prev_value * (1 + calcolo_interesse_mensile)
            
            if i < self.investment_months:
                new_value = base_value + self.config['params']['importo_PAC_iniziale']
                new_capital = prev_capital + self.config['params']['importo_PAC_iniziale']
            else:
                if month == self.config['params']['mese_ribilanciamento']:
                    new_value = base_value + self.config['params']['investimento_annuale_ribilanciamento_stimato']
                    new_capital = prev_capital + self.config['params']['investimento_annuale_ribilanciamento_stimato']
                else:
                    new_value = base_value
                    new_capital = prev_capital
                    
            self.investment_projections.loc[i] = [date, new_value, new_capital]

    def generate_predictions(self):
        """Generate final predictions and summaries."""
        if not hasattr(self, 'merged'):
            raise ValueError("Merged data not prepared")
        
        # Calculate liquidity requirements
        self.merged['Liquidita_ipotetica'] = (
            self.merged['Rolling_Spesa_Mean'] * 
            self.config['params']['mesi_di_spese_correnti']
        )

        self.merged['Conto_deposito'] = (
            self.merged['Rolling_Spesa_Mean'] * 
            self.config['params']['mesi_di_spese_impreviste']
        )
        
        # Calculate future savings
        self.merged['Future_savings_sum'] = (
            self.savings + 
            self.merged['Totale'].cumsum()
        )
        
        # Calculate investment projections
        self.calculate_investment_projections()
        
        # Generate final summary
        self.final_summary = pd.merge(
            self.merged[['YearMonth', 'Future_savings_sum', 'Conto_deposito','Liquidita_ipotetica']], 
            self.investment_projections,
            on='YearMonth'
        )
        
        self.final_summary['Patrimonio_futuro_netto'] = (
            self.final_summary['Future_savings_sum'] -
            self.final_summary['Capitale_Investito'] +
            self.final_summary['Valore_investimento_Fineco'] -
            self.final_summary.apply(
                lambda row: max((row['Valore_investimento_Fineco'] - row['Capitale_Investito']) * 0.26, 0),
                axis=1
            ))

        self.final_summary['Patrimonio_other'] = (
            self.final_summary['Future_savings_sum'] - 
            self.final_summary['Capitale_Investito'] -
            self.final_summary['Conto_deposito']
        )

        self.final_summary['difference'] = (
            self.final_summary['Future_savings_sum'] - 
            self.final_summary['Liquidita_ipotetica'] -
            self.final_summary['Capitale_Investito'] -
            self.final_summary['Conto_deposito']
        )

        self.final_summary['Capitale_Investito'] = (
            self.final_summary['Capitale_Investito'] + 
            np.minimum(self.final_summary['difference'], 0)
        )

        self.final_summary['Valore_investimento_Fineco'] = (
            self.final_summary['Valore_investimento_Fineco'] + 
            np.minimum(self.final_summary['difference'], 0)
        )

        self.final_summary['Patrimonio_futuro_netto'] = (
            self.final_summary['Patrimonio_futuro_netto'] + 
            np.minimum(self.final_summary['difference'], 0)
        )

        # logging.info("Final summary tables:")
        # logging.info("%s", self.final_summary.head(10))

        # Get first available date from final_summary
        dates_plot = []
        
        # Get first date
        first_date = self.final_summary.iloc[0]['YearMonth']
        dates_plot.append(first_date)
        
        # Get every 12th date after that until the end
        for i in range(12, len(self.final_summary), 12):
            dates_plot.append(self.final_summary.iloc[i]['YearMonth'])

        # logging.info("Date per il plot:")
        # logging.info("%s", dates_plot)
        
        plot_final = pd.DataFrame({'YearMonth': dates_plot})
        final_summarized = pd.merge(plot_final, self.final_summary, on='YearMonth')
        
        colonne_serie = [
            'YearMonth',
            'Capitale_Investito',
            'Valore_investimento_Fineco', 
            'Liquidita_ipotetica',
            'Conto_deposito',
            'Patrimonio_futuro_netto'
        ]
        
        final_summarized = final_summarized[colonne_serie].rename(columns={
            'Capitale_Investito': 'Capitale investito',
            'Valore_investimento_Fineco': 'Controvalore Fineco',
            'Liquidita_ipotetica': 'Capitale cash', 
            'Conto_deposito': 'Fondo spese imprevedibili', 
            'Patrimonio_futuro_netto': 'Patrimonio netto stimato'
        })
        
        final_summarized['YearMonth'] = final_summarized['YearMonth'].dt.strftime('%Y-%m')
        
        # logging.info("Final summarized predictions:")
        # logging.info("%s", final_summarized.head(10))
        
        self.final_summary = final_summarized

    def process_table_values(self, table, round_digits=0):
        """Process table values for display, rounding as necessary."""
        return [[value if not isinstance(value, float) or pd.isna(value)
                else int(round(value)) if round_digits == 0
                else round(value, round_digits)
                for value in row] for row in table.values.tolist()]

    def process_risk_table(self, df, sort_col, drop_col=None, scale=1, decimals=2, num_raws=10):
        """Process a risk table, sorting and scaling as needed."""
        # # Add debug log to check column names
        # logging.info("Available columns in DataFrame: %s", df.columns.tolist())
        
        # Ensure the DataFrame has the expected columns
        if sort_col not in df.columns:
            logging.warning("'%s' not found in columns. Using first numeric column for sorting.", sort_col)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                sort_col = numeric_cols[0]
            else:
                raise ValueError("No numeric columns found for sorting")
        
        df = df.sort_values(sort_col, ascending=False).head(num_raws)
        if drop_col and drop_col in df.columns:
            df = df.drop(columns=drop_col)
        if scale != 1:
            df.loc[:, sort_col] *= scale
        return df.columns.tolist(), self.process_table_values(df, round_digits=decimals)

    def generate_report(self):
        """Generate the financial report based on processed data."""
        # Call check_yield and store the message
        xeon_message = self.check_yield()

        # Process main tables
        columns_1 = self.income_pivot.columns.tolist()
        rows_1 = self.process_table_values(self.income_pivot)
        rows_2 = self.process_table_values(self.expense_pivot)
        rows_3 = self.process_table_values(self.savings_pivot)

        # Process detail table
        columns_detail = self.detail_table.columns.tolist()
        rows_detail = self.process_table_values(self.detail_table)

        # Process future predictions table
        columns_future = self.final_summary.columns.tolist()
        rows_future = self.process_table_values(self.final_summary)

        # Process risk tables
        columns_currency_risk, rows_currency_risk = self.process_risk_table(
            self.currency_risk, 'Risk Percentage', drop_col='Currency Exposure')
        
        columns_country_risk, rows_country_risk = self.process_risk_table(
            self.country_risk, 'Risk Percentage', drop_col='Country Exposure')
        
        columns_sector_new, rows_sector_new = self.process_risk_table(
            self.sector_weights[['Sector','Percentage']], 'Percentage', scale=100)
        
        columns_holdings_new, rows_holdings_new = self.process_risk_table(
            self.holdings_weights[['Ticker','holdingName','Percentage']], 'Percentage', scale=100, num_raws=21)

        # Read images
        with open("histogram_last_month.jpg", "rb") as f:
            image_data_hist = base64.b64encode(f.read()).decode()

        with open("Trend_investimenti.jpg", "rb") as f:
            trend_investimenti = base64.b64encode(f.read()).decode()

        with open("Portfolio_Allocation.jpg", "rb") as f:
            portfolio_allocation = base64.b64encode(f.read()).decode()

        # Load and render template
        env = Environment(loader=FileSystemLoader("."))
        template = env.from_string(self.template_html)
        
        html = template.render(
            risp_fino_ad_ora=int(round(self.savings, 0)),
            patrimonio_ad_oggi=int(round(self.current_net_assets, 0)),
            Rendimento_attuale_fineco_lordo=self.gross_return,
            columns_1=columns_1,
            rows_1=rows_1,
            rows_2=rows_2,
            rows_3=rows_3,
            columns_detail=columns_detail,
            rows_detail=rows_detail,
            last_18_months_mean_spese=int(round(self.mean_expenses, 0)),
            last_18_months_mean_reddito=int(round(self.mean_income, 0)),
            last_18_months_mean_saving=int(round(self.mean_savings, 0)),
            last_12_months_mean_Fede=int(round(self.fede_mean, 0)),
            last_12_months_mean_Anna=int(round(self.anna_mean, 0)),
            Valore_medio_assegno_unico=int(round(self.bonus_mean, 0)),
            values_bimbo_mean=int(round(self.child_expenses_mean, 0)),
            capitale_investito_Fineco=int(round(self.fineco_invested, 0)),
            controvalore_investimento=int(round(self.investment_value_portfolio, 0)),
            Liquidita_attuale=int(round(self.current_liquidity, 0)),
            Liquidita_ideale=int(round(self.ideal_liquidity, 0)),
            Conto_deposito_attuale=int(round(self.investment_deposit_account, 0)),
            Conto_deposito_ideale=int(round(self.deposit_account_ideal, 0)),
            Importo_da_investire=int(round(self.investable_amount, 0)),
            anni_futuri=self.config['params']['anni_per_la_previsione']-1,
            data_cambio_lavoro_Anna=self.config['params']['data_cambio_lavoro_Anna'],
            data_cambio_casa=self.config['params']['data_cambio_casa'],
            data_viaggio_america=self.config['params']['data_viaggio_america'],
            investimento_annuale_ribilanciamento_stimato=self.config['params']['investimento_annuale_ribilanciamento_stimato'],
            interesse_lordo_Portafoglio_azionario=int(round(self.config['params']['interesse_lordo_Portafoglio_azionario'], 0)),
            columns_future=columns_future,
            rows_future=rows_future,
            image_data_hist=image_data_hist,
            Trend_investimenti=trend_investimenti,
            allocazone_portafoglio=portfolio_allocation,
            columns_currency_risk=columns_currency_risk,
            rows_currency_risk=rows_currency_risk,
            columns_country_risk=columns_country_risk,
            rows_country_risk=rows_country_risk,
            columns_max_sector_new=columns_sector_new,
            rows_max_sector_new=rows_sector_new,
            columns_max_holdings_new=columns_holdings_new,
            rows_max_holdings_new=rows_holdings_new,
            num_securities_held=self.num_securities,
            xeon_message=xeon_message  # Pass the message to the template
        )

        with open("output.html", "w") as f:
            f.write(html)
            
        # Send first email
        utils.send_email("output.html", token_gmail.first_mail_to, self.update)
        
        # Ask if second email should be sent
        send_second = input("Send email to Anna? (Y/N): ")
        if send_second.upper() == 'Y':
            utils.send_email("output.html", token_gmail.second_mail_to, self.update)

    def check_yield(self, ticker="XEON.MI"):
        """Calculate the yield for a specific ticker from 30 days ago to today."""
        try:
            desired_interest = self.desired_interest
            # Fetch historical data for the ticker
            ticker_data = Ticker(ticker)
            historical_data = ticker_data.history(period="30d")
            
            # Calculate yield
            if len(historical_data) < 2:
                logging.warning("Not enough data to calculate yield for %s", ticker)
                return "Dati insufficienti per il calcolo del rendimento."
            
            start_price = historical_data['close'].iloc[0]
            end_price = historical_data['close'].iloc[-1]
            yield_value = (end_price / start_price) - 1

            desired_return = 1+desired_interest/100
            
            # Compare with the threshold
            threshold = desired_return**(1/12) - 1
            if yield_value > threshold:
                return "<span style='color:blue;'>Prosegui con Xeon</span>"
            else:
                return "<span style='color:red;'>Sostituire Xeon magari con Ibonds</span>"
        
        except Exception as e:
            logging.error("Error calculating yield for %s: %s", ticker, e)
            return "Errore nel calcolo del rendimento."

def main():
    """Main function to execute financial analysis."""
    analysis = FinancialAnalysis()
    analysis.read_and_process_data()
    analysis.create_pivot_tables()
    analysis.process_tables()
    analysis.save_and_process_table()
    analysis.calculate_inflation_rate()
    analysis.calculate_metrics()
    analysis.process_portfolio()
    
    # Initialize future projections with required data
    future_proj = utils.FutureProjections(
        income=analysis.income, 
        expenses=analysis.expenses,
        expense_details=analysis.all_data[analysis.all_data['Tipo']=='Expense'],
        config=analysis.config,
        inflation_rate=analysis.inflation_rate
    )

    # Calculate future projections in sequence
    future_proj.calculate_future()  # Base projections
    future_proj.add_tax_payment()   # Add any tax payments
    
    # Calculate child expenses including daycare/school
    child_future = future_proj.calculate_child_expenses()
    
    # Calculate job change impact
    job_change = future_proj.calculate_job_change()
    
    # Calculate housing change impact
    housing_change = future_proj.calculate_housing_change()
    
    # Calculate travel expenses increase
    travel_values = future_proj.calculate_travel_increase()
    travel_values = future_proj.add_america_trip(travel_values)
    
    # Calculate maternity impact
    future_proj.calculate_maternity(analysis.anna_mean)
    
    # Combine all future expenses
    future_proj.combine_expenses(
        travel_values=travel_values,
        job_change=job_change, 
        housing_change=housing_change,
        child_future=child_future
    )

    # Store results for report generation
    analysis.merged = future_proj.merged
    analysis.calculate_rolling_means()
    analysis.generate_predictions()
    analysis.generate_report()

if __name__ == "__main__":
    main()
