import warnings
import pandas as pd
import token_gmail
import os
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import yfinance as yf
from yahooquery import Ticker
from fuzzywuzzy import fuzz
from dateutil.relativedelta import relativedelta
import logging
from smtplib import SMTP
from email.mime.text import MIMEText
import glob
import time
from requests.exceptions import ChunkedEncodingError
import zipfile
import sqlite3
import shutil

# Suppress specific warnings
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_email(path_html: str, mail_to: str, update: str) -> None:
    """
    Send HTML email to specified recipient.
    
    Args:
        path_html: Path to HTML file to send
        mail_to: Recipient email address 
        update: Update date string
        
    Returns:
        None
    
    Raises:
        FileNotFoundError: If HTML file not found
        SMTPException: If error sending email
    """
    try:
        # Read HTML content
        with open(path_html, "r") as f:
            html_content = f.read()
            
        # Configure message
        message = MIMEText(html_content, "html")
        message['Subject'] = f'Monthly Expense Update: {update}'
        message['From'] = token_gmail.mail_from
        message['To'] = mail_to
        
        # Configure and send via SMTP
        with SMTP('smtp.gmail.com:587', timeout=30) as server:
            server.starttls()
            server.login(token_gmail.mail_from, token_gmail.pat_gmail)
            server.send_message(message)
            
        logging.info(f"Email sent successfully to {mail_to}")
        
    except FileNotFoundError:
        logging.error(f"HTML file not found: {path_html}")
        raise
    except SMTPException as e:
        logging.error(f"SMTP error sending email: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Generic error sending email: {str(e)}")
        raise

class FutureProjections:
    """Class to calculate and analyze future financial projections"""
    
    def __init__(self, income, expenses, expense_details, config, inflation_rate):
        """Initialize with income, expense and config data
        
        Args:
            income: DataFrame containing income data
            expenses: DataFrame containing expenses data
            expense_details: DataFrame containing detailed expense information
            config: Configuration parameters
            inflation_rate: Current inflation rate
        """
        self.expense_details = expense_details
        self.income = income.reset_index()
        self.expenses = expenses.reset_index()
        self.merged = pd.merge(self.income, self.expenses, on='YearMonth').tail(12)
        self.inflation_rate = inflation_rate
        # Check if merged DataFrame is empty
        if self.merged.empty:
            raise ValueError("No data found after merging income and expenses")
        
        self.max_date = pd.to_datetime(self.income['YearMonth'].max())
        self.config = config
        self.projection_years = self.config['params']['anni_per_la_previsione']
        self.end_date = self.max_date + pd.DateOffset(years=self.projection_years)
        self.next_month = self.max_date + pd.DateOffset(months=1)
        
        # Calculate investment months
        data_fine_PAC_iniziale = pd.to_datetime(self.config['params']['data_fine_PAC_iniziale'])
        data_ultima_tranche = pd.to_datetime(self.max_date)
        self.investment_months = int((data_fine_PAC_iniziale - data_ultima_tranche).days / 30.44)
        
        # Calculate invested capital from Fineco investments
        fineco_investimento = expense_details[
            (expense_details['Tipo'] == "Expense") &
            (expense_details['Categoria'] == "Finance") &
            (expense_details['SottoCategoria'].isin(["Fineco"]))
        ]

        # Calculate invested capital from Fineco investments
        conto_deposito_investimento = expense_details[
            (expense_details['Tipo'] == "Expense") &
            (expense_details['Categoria'] == "Finance") &
            (expense_details['SottoCategoria'].isin(["Conto deposito"]))
        ]

        self.invested_capital = fineco_investimento['Importo'].sum()
        
        # Initialize investment value (will be set later)
        self.investment_value = 0

    def _adjust_income(self):
        """Adjust known income and expenses for specific dates"""
        expense_corrections = self.config['correzioni']['spese']
        income_corrections = self.config['correzioni']['reddito']
        
        for date_str, value in expense_corrections.items():
            date = pd.to_datetime(date_str)
            if date in self.merged['YearMonth'].values:
                self.merged.loc[self.merged['YearMonth'] == date, 'Expense'] = value
                
        for date_str, value in income_corrections.items():
            date = pd.to_datetime(date_str)
            if date in self.merged['YearMonth'].values:
                self.merged.loc[self.merged['YearMonth'] == date, 'Income'] = value

    def calculate_future(self):
        """Calculate future projections based on historical data"""
        self._adjust_income()
        self.merged['YearMonth'] = pd.to_datetime(self.merged['YearMonth'])
        
        # Get inflation rate from config
        inflation_rate = self.inflation_rate / 100
        
        # Create future dates range
        future_dates = pd.date_range(
            start=self.next_month,
            end=self.end_date,
            freq='MS'
        )
        
        # Verify dates are being generated correctly
        if len(future_dates) == 0:
            logging.warning("WARNING: No future dates generated!")
            logging.info(f"next_month: {self.next_month}")
            logging.info(f"end_date: {self.end_date}")
        else:
            logging.info(f"First date in series: {future_dates[0]}")
            logging.info(f"Last date in series: {future_dates[-1]}")
            logging.info(f"Number of future dates: {len(future_dates)}")
        
        # Create future DataFrame
        future = pd.DataFrame({'YearMonth': future_dates})
        
        # Loop through future dates
        for i, row in future.iterrows():
            year = row['YearMonth'].year
            month = row['YearMonth'].month
            
            # Get values from same month in previous year for both income and expenses
            prev_year_data = self.merged[
                (self.merged['YearMonth'].dt.year == year-1) & 
                (self.merged['YearMonth'].dt.month == month)
            ]
            
            if not prev_year_data.empty:
                reddito = prev_year_data['Income'].values[0]
                # Add inflation to previous year's expenses
                spese = prev_year_data['Expense'].values[0] * (1 + inflation_rate)
            else:
                # Use average if no data for previous year
                reddito = self.merged['Income'].mean()
                spese = self.merged['Expense'].mean() * (1 + inflation_rate)
            
            # Add row to merged DataFrame
            future_row = pd.DataFrame({
                'YearMonth': [row['YearMonth']], 
                'Income': [reddito],
                'Expense': [spese]
            })
            self.merged = pd.concat([self.merged, future_row], ignore_index=True)

        # Remove any YearMonth from self.merged that is not in future_dates
        self.merged = self.merged[self.merged['YearMonth'].isin(future_dates)]
        
        return self.merged

    def add_tax_payment(self):
        """Add hypothetical tax payment on specified date"""
        tax_date = pd.to_datetime(self.config['params']['data_agenzia_entrate'])
        tax_amount = self.config['params']['importo_tasse_agenzia']
        
        mask = self.merged['YearMonth'] == tax_date
        self.merged.loc[mask, 'Expense'] += tax_amount
        
        self.merged = (self.merged
                      .sort_values(by='YearMonth')
                      .reset_index(drop=True))
        
        return self.merged
    
    def calculate_child_expenses(self):
        """Calculate future child expenses including daycare and school"""
        # Load configuration parameters
        daycare_cost = self.config['params']['costo_asilo_nido_regime']  # Base cost of daycare
        benefits = self.config['params']['aiuto_luxottica_e_bonus_nido']  # Benefits/subsidies received
        school_cost = self.config['params']['costo_retta_scuola_stimata']  # Estimated school cost
        start_date = pd.to_datetime(self.config['params']['start_nido_date'])  # Daycare start date
        end_date = pd.to_datetime(self.config['params']['end_nido_date'])  # Daycare end date
        no_fee_month = self.config['params']['mese_senza_retta_scolastica']  # Month without fees
        inflation_rate = self.inflation_rate / 100  # Convert inflation rate to decimal

        # Calculate net daycare cost after benefits
        net_daycare_cost = daycare_cost - benefits
        
        # Determine date range for future calculations
        # If start date is in the future, use it as start point
        # Otherwise use next month as start point
        if start_date > pd.to_datetime(self.max_date):
            date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
        else:
            date_range = pd.date_range(start=self.next_month, end=self.end_date, freq='MS')
            
        # Create DataFrame to store future child expenses
        future_child = pd.DataFrame({'YearMonth': date_range})
        
        # Get historical daycare expenses to calculate elapsed time
        daycare_expenses = self.expense_details[
            (self.expense_details['Categoria']=='Bimbo') & 
            (self.expense_details['SottoCategoria']=='Asilo Nido')
        ]
        daycare_expenses['YearMonth'] = pd.to_datetime(daycare_expenses['YearMonth'])
        max_daycare_date = daycare_expenses['YearMonth'].max()
        
        # Calculate months elapsed since start date
        if pd.notna(max_daycare_date):
            diff = relativedelta(max_daycare_date, start_date)
            months_elapsed = diff.years * 12 + diff.months
        else:
            months_elapsed = 0

        # Calculate base expenses for each month
        for index, row in future_child.iterrows():
            month = row['YearMonth'].strftime('%m')
            
            # No fees in specified month
            if month == no_fee_month:
                expense = 0
            # During daycare period
            elif row['YearMonth'] <= end_date:
                # Only charge for first 12 months of daycare
                if months_elapsed < 12 and index < (11-months_elapsed):
                    expense = net_daycare_cost
                else:
                    expense = 0
            # After daycare period (school period)
            else:
                # Switch from daycare to school costs, except in no-fee month
                expense = -net_daycare_cost + school_cost if month != no_fee_month else 0
                
            future_child.loc[index, 'Incremento_costo_bimbo'] = expense

        # Apply inflation to expenses year over year
        for i, row in future_child.iterrows():
            year = row['YearMonth'].year
            month = row['YearMonth'].month
            
            # Get previous year's expense for same month
            prev_year_data = future_child[
                (pd.to_datetime(future_child['YearMonth']).dt.year == year-1) & 
                (pd.to_datetime(future_child['YearMonth']).dt.month == month)
            ]
            
            # Apply inflation if there was a previous year's expense
            if not prev_year_data.empty and prev_year_data['Incremento_costo_bimbo'].values[0] > 0:
                current_expense = future_child.loc[i, 'Incremento_costo_bimbo']
                # Only apply inflation to positive expenses
                if current_expense > 0:
                    future_child.loc[i, 'Incremento_costo_bimbo'] = current_expense * (1 + inflation_rate)

        return future_child

    def calculate_job_change(self):
        """Calculate impact of job change on future finances"""
        # Log key dates for debugging
        logging.info(f"max_date: {self.max_date}")
        logging.info(f"projection_years: {self.projection_years}")
        logging.info(f"end_date: {self.end_date}")
        
        # Get job change parameters from config
        change_date = self.config['params']['data_cambio_lavoro_Anna']
        amount = self.config['params']['importo_cambio_lavoro_Anna']
        change_date = pd.to_datetime(change_date)
        
        # Calculate one year after change date
        one_year_after = change_date + pd.DateOffset(years=1)
        
        # If job change is in the future
        if change_date > self.max_date:
            # Create monthly date range from change date to end date
            date_range = pd.date_range(
                start=change_date,
                end=self.end_date, 
                freq='MS'  # MS = Month Start frequency
            )
            
            # Create DataFrame with monthly job change amounts
            df = pd.DataFrame({
                'YearMonth': date_range,
                'Cambio_lavoro_Anna': amount  # Fixed amount for each month
            })
            
            return df
        
        # If job change is in the past but within the first year
        elif change_date <= self.max_date and one_year_after > self.max_date:
            # Create monthly date range from current date to one year after change date
            date_range = pd.date_range(
                start=self.max_date,
                end=one_year_after,
                freq='MS'
            )
            
            # Create DataFrame with monthly job change amounts
            df = pd.DataFrame({
                'YearMonth': date_range,
                'Cambio_lavoro_Anna': amount
            })
            
            logging.info("Applying job change amount for remaining months in first year")
            return df
        
        # If change date is more than one year in the past, return empty DataFrame
        df = pd.DataFrame()
        logging.info("Job change more than one year in the past, no impact on future projections")
        return df
    
    def calculate_housing_change(self):
        """Calculate impact of housing change on future finances"""
        change_date = pd.to_datetime(self.config['params']['data_cambio_casa'])
        amount = self.config['params']['importo_cambio_casa']

        if change_date > self.max_date:
            date_range = pd.date_range(
                start=change_date,
                end=self.end_date,
                freq='MS'
            )
            
            result = pd.DataFrame({
                'YearMonth': date_range,
                'Cambio_casa': amount
            })
            
            return result

        logging.info("No future house changes planned")
        return pd.DataFrame()
    
    def calculate_travel_increase(self):
        """Calculate increased travel costs for future projections"""
        # Get inflation rate from config
        inflation_rate = self.inflation_rate / 100
        
        # Get travel expenses and group by month
        viaggi_cat = self.expense_details[self.expense_details['Categoria'] == 'Travel']
        values_viaggi = viaggi_cat[['YearMonth', 'Importo']].groupby(['YearMonth']).sum()
        values_viaggi = values_viaggi.reset_index()

        # Convert 'YearMonth' column to datetime
        values_viaggi['YearMonth'] = pd.to_datetime(values_viaggi['YearMonth'], format='%Y-%m')

        # Filter the data based on the condition
        values_viaggi = values_viaggi[values_viaggi['YearMonth'] > (pd.to_datetime(self.merged.loc[0]['YearMonth']) - pd.DateOffset(years=1))]

        # Perform initial calculations with travel impact percentage
        values_viaggi['Viaggi_maggiorato'] = round(values_viaggi['Importo'] * self.config['params']['impatto_sui_viaggi_percento'] / 100, 2)

        # Select the columns needed
        values_viaggi = values_viaggi[['YearMonth', 'Viaggi_maggiorato']]

        # Create a new DataFrame with the desired date range
        viaggi_dates = pd.date_range(start=self.next_month, end=self.end_date, freq='MS')
        viaggi = pd.DataFrame({'YearMonth': viaggi_dates})

        # Initialize future values
        future_values = []
        
        for date in viaggi_dates:
            # Find value from same month in previous year
            prev_year_date = date - pd.DateOffset(years=1)
            prev_year_value = values_viaggi[values_viaggi['YearMonth'].dt.month == prev_year_date.month]['Viaggi_maggiorato']
            
            if not prev_year_value.empty:
                # Take the most recent value for that month and apply inflation
                value = prev_year_value.iloc[-1] * (1 + inflation_rate)
            else:
                # If no previous value exists for this month, use 0
                value = 0.0
                
            future_values.append({
                'YearMonth': date,
                'Viaggi_maggiorato': round(value, 2)
            })
            
            # Immediately add this value to values_viaggi for next iterations
            values_viaggi = pd.concat([
                values_viaggi, 
                pd.DataFrame([{'YearMonth': date, 'Viaggi_maggiorato': round(value, 2)}])
            ], ignore_index=True)
        
        # Filter dates after the first date in merged DataFrame
        values_viaggi = values_viaggi[values_viaggi['YearMonth'] > pd.to_datetime(self.merged.loc[0]['YearMonth'])]

        return values_viaggi

    def add_america_trip(self, travel_values):
        """Add America trip cost to future expense projections
        
        Args:
            travel_values: DataFrame containing future travel values
        
        Returns:
            DataFrame with added trip cost
        """
        trip_date = pd.to_datetime(self.config['params']['data_viaggio_america'])
        trip_cost = self.config['params']['importo_viaggio_america']

        # Only add trip if it's in the future
        if trip_date > self.max_date:
            # Initialize column with zeros
            travel_values['Viaggio America'] = 0
            # Set cost only for matching date
            mask = travel_values['YearMonth'] == trip_date
            travel_values.loc[mask, 'Viaggio America'] = trip_cost

        return travel_values

    def calculate_maternity(self, last_12_months_mean):
        """Calculate salary reduction during maternity leave
        
        Args:
            last_12_months_mean: Average salary over the last 12 months
        
        Returns:
            None
        """
        leave_months = self.config['params']['mesi_maternita_facoltativa_Anna']
        reduced_salary = int(round(last_12_months_mean * 0.70, 0))
        # Convert start_date to a datetime64 object if it's not already
        start_date = np.datetime64(self.config['params']['start_date_maternity'])
        
        # Change the addition to use a timedelta in days instead of months
        end_date = start_date + np.timedelta64(leave_months * 30, 'D')  # Approximate month as 30 days
        
        # Verifica logica basata su self.max_date
        if self.max_date < start_date and leave_months == 2:
            maternity_dates = [start_date, start_date + np.timedelta64(1, 'M')]
        elif self.max_date < end_date:
            maternity_dates = [end_date]
        else:
            logging.info('For Maternity: Salary already reduced')
            return
        
        # Applica la riduzione salariale per le date calcolate
        for date in maternity_dates:
            formatted_date = np.datetime_as_string(date, unit='M')  # Converte in 'YYYY-MM'
            self.merged.loc[self.merged['YearMonth'] == formatted_date, 'Incremento_costo_bimbo'] = reduced_salary

        logging.info('Maternity salary reduction applied for the specified months.')

    def combine_expenses(self, travel_values, job_change, housing_change, child_future):
        """Combine all future expenses and calculate totals
        
        Args:
            travel_values: DataFrame containing future travel values
            job_change: DataFrame containing job change values
            housing_change: DataFrame containing housing change values
            child_future: DataFrame containing future child expenses
        
        Returns:
            None
        """
        # Only filter travel values if not empty
        if not travel_values.empty:
            travel_values = travel_values[travel_values['YearMonth'] > self.max_date]
        
        # Store original merged data
        result = self.merged.copy()
        
        # Perform merges only if DataFrames are not empty
        dataframes = [
            (child_future, 'YearMonth', 'left') if not child_future.empty else None,
            (job_change, 'YearMonth', 'left') if not job_change.empty else None,
            (housing_change, 'YearMonth', 'left') if not housing_change.empty else None,
            (travel_values, 'YearMonth', 'left') if not travel_values.empty else None
        ]
        
        for df_tuple in dataframes:
            if df_tuple is not None:
                df, on, how = df_tuple
                result = pd.merge(result, df, on=on, how=how)
        
        # Fill NaN values with 0
        result = result.fillna(0)
        
        # Verify required columns exist before calculation
        required_columns = ['Income', 'Expense', 'Incremento_costo_bimbo', 
                           'Cambio_lavoro_Anna', 'Cambio_casa', 
                           'Viaggi_maggiorato', 'Viaggio America']
                           
        missing_columns = [col for col in required_columns if col not in result.columns]
        if missing_columns:
            logging.error("Original merged columns:", self.merged.columns.tolist())
            logging.error("Final result columns:", result.columns.tolist())
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        self.merged = result
        
        # Rest of the method remains the same...
        self.merged['Totale'] = (
            self.merged['Income'] 
            - self.merged['Expense']
            - self.merged['Incremento_costo_bimbo']
            + self.merged['Cambio_lavoro_Anna']
            - self.merged['Cambio_casa']
            - self.merged['Viaggi_maggiorato']
            - self.merged['Viaggio America']
        )
        
        self.merged['Spesa_new'] = (
            self.merged['Expense']
            + self.merged['Incremento_costo_bimbo']
            - self.merged['Cambio_lavoro_Anna']
            + self.merged['Cambio_casa']
            + self.merged['Viaggi_maggiorato']
            + self.merged['Viaggio America']
        )
        
        window_size = 12
        self.merged['Rolling_Spesa_Mean'] = (
            self.merged['Spesa_new']
            .rolling(window=window_size, min_periods=0)
            .mean()
            .shift(-window_size + 1)
        )
        
        self.merged['Rolling_Spesa_Mean'].fillna(
            self.merged['Rolling_Spesa_Mean'].mean(), 
            inplace=True
        )
        
        self.merged['Liquidita_ipotetica'] = (
            self.merged['Rolling_Spesa_Mean'] 
            * self.config['params']['mesi_di_spese_correnti']
        )

        self.merged['Conto_deposito'] = (
            self.merged['Rolling_Spesa_Mean'] 
            * self.config['params']['mesi_di_spese_impreviste']
        )

        # logging.info("Future liquidita ipotetica:")
        # logging.info(self.merged.head(15))

    def _calculate_future_savings(self, savings):
        """Calculate future savings projections"""
        future = self.merged[['YearMonth','Totale','Liquidita_ipotetica', 'Conto_deposito']]
        future['Future_savings_sum'] = savings + future['Totale'].cumsum()

        # logging.info("Future savings:")
        # logging.info(future.head(15))

        return future.drop('Totale', axis=1)

    def _calculate_investment_amounts(self, dates, diff_months, base_capital, 
                                   recurring_inv, annual_inv, inv_month):
        """Calculate future investment amounts"""
        investments = pd.DataFrame(columns=['YearMonth', 'Capitale_Investito'])
        
        for i, date in enumerate(dates):
            capital = base_capital
            
            if i == 0:
                if diff_months > 0:
                    capital += recurring_inv
            elif i < diff_months:
                capital = investments.loc[i-1, 'Capitale_Investito'] + recurring_inv
            else:
                capital = investments.loc[i-1, 'Capitale_Investito']
                month = np.datetime_as_string(date.astype('datetime64[M]'), unit='M')[-2:]
                if month == inv_month:
                    capital += annual_inv
                    
            investments.loc[i] = [date, round(capital,2)]
            
        return investments

    def _calculate_investment_values(self, dates, diff_months, base_value,
                                  base_capital, monthly_interest, recurring_inv,
                                  annual_inv, inv_month, future_investment):
        """Calculate future investment values including returns"""
        investments = pd.DataFrame(columns=['YearMonth', 'Valore_investimento_Fineco',
                                         'Capitale_Investito'])
        
        for i, date in enumerate(dates):
            value = base_value
            capital = base_capital
            
            if i > 0:
                value = investments.loc[i-1, 'Valore_investimento_Fineco'] * (1 + monthly_interest)
                capital = investments.loc[i-1, 'Capitale_Investito']
                
                if i < diff_months:
                    value += recurring_inv
                    capital += recurring_inv
                else:
                    month = np.datetime_as_string(date.astype('datetime64[M]'), unit='M')[-2:]
                    liquidity = future_investment.loc[i, 'Liquidita_Reale']
                    if month == inv_month and liquidity > 0:
                        value += annual_inv
                        capital += annual_inv
                        
            investments.loc[i] = [date, round(value,2), round(capital,2)]
            
        return investments

    def _format_summary(self, summary):
        """Format the final summary dataframe"""
        dates = pd.date_range(start=self.next_month, end=self.end_date, 
                            freq=pd.DateOffset(years=1))
        
        final = pd.merge(
            pd.DataFrame({'YearMonth': dates}),
            summary,
            on='YearMonth'
        )
        
        columns = ['YearMonth', 'Capitale_Investito', 'Valore_investimento_Fineco',
                  'Patrimonio_other', 'Patrimonio_futuro_lordo']
                  
        column_names = {
            'Capitale_Investito': 'Invested Capital',
            'Valore_investimento_Fineco': 'Investment Value', 
            'Patrimonio_other': 'Non-Invested Capital',
            'Patrimonio_futuro_lordo': 'Estimated Gross Assets'
        }
        
        final = final[columns].rename(columns=column_names)
        final['YearMonth'] = final['YearMonth'].dt.strftime('%Y-%m')
        
        # logging.info("Final summary formatted:")
        # logging.info(final.head(10))
        
        return final


def analyze_portfolio(df, config_portfolio, countries, currency, stock_price, config):
    """Analyze portfolio data and calculate risk metrics.
    
    Args:
        df: Portfolio transaction data
        config_portfolio: Portfolio configuration
        countries: Country data
        currency: Currency data
        stock_price: Current stock price
        config: General configuration
        
    Returns:
        tuple: Portfolio metrics and risk analysis
    """
    df = _clean_portfolio_data(df,config_portfolio)

    deposit_account_df = df[df['Ticker']=='XEON.MI']
    portfolio_df = df[df['Ticker']!='XEON.MI']

    deposit_account_portfolio = _calculate_portfolio_metrics(deposit_account_df)


    countries = _process_country_currency_data(countries, currency)
    portfolio = _calculate_portfolio_metrics(portfolio_df)
    total_value = _calculate_total_value(portfolio, config, stock_price)

    total_value_deposit_account = _calculate_total_value(deposit_account_portfolio, config, stock_price=0)
    num_securities = len(portfolio_df['Ticker'].unique())


    
    currency_risk = _calculate_currency_risk(portfolio, config_portfolio, countries)
    country_risk = _calculate_country_risk(portfolio, countries)
    sector_risk = _calculate_sector_risk(portfolio)
    holdings_risk = _calculate_holdings_risk(portfolio)
    
    _generate_portfolio_trend(portfolio_df)
    
    logging.info('Portfolio analysis complete')
    return (total_value, total_value_deposit_account, num_securities, currency_risk, 
            country_risk, sector_risk, holdings_risk, portfolio_df)

def _clean_portfolio_data(df,config_portfolio):
    """Clean and format portfolio data"""
    df = df.dropna(subset=['Descrizione'])
    df['Operation'] = pd.to_datetime(df['Operazione'], format='%d/%m/%Y').drop(columns=['Operazione'])
    df['Value Date'] = pd.to_datetime(df['Data valuta'], format='%d/%m/%Y').drop(columns=['Data valuta'])
    
    numeric_cols = ['Quantita', 'Prezzo', 'Cambio', 'Controvalore', 
                   'Commissioni Fondi Sw/Ingr/Uscita']
    
    for col in numeric_cols:
        df[col] = (df[col]#.astype(str)
                        # .str.replace('.', '')
                        # .str.replace(',', '.')
                        .astype(float))
                         
    config_df = pd.DataFrame(config_portfolio['etf'], 
                           columns=['Ticker', 'TER', 'ISIN'])
    df = df.merge(config_df, how='left', on='ISIN')
    
    return df

def _process_country_currency_data(countries, currency):
    """Process and merge country and currency data"""
    df1 = countries
    df2 = currency
    
    matches = {}
    for country1 in df1['countries'].unique():
        max_similarity = 0
        best_match = None
        for country2 in df2['countries']:
            similarity = calculate_similarity(country1, country2)
            if similarity > max_similarity:
                max_similarity = similarity
                best_match = country2
        matches[country1] = best_match

    df1['matched_country'] = df1['countries'].map(matches)
    df1 = pd.merge(df1, df2, left_on='matched_country', right_on='countries')
    
    return df1[['symbol','countries_x','percentage','currency']].drop_duplicates().rename(
        columns={'countries_x':'countries'})

def _calculate_portfolio_metrics(df):
    """Calculate key portfolio metrics"""
    portfolio_data = []

    df = df.dropna(subset=['Descrizione'])

    for ticker in df['Ticker'].unique():
        ter = df[df['Ticker'] == ticker]['TER'].iloc[0]
        current_price = Ticker(ticker).price[ticker]['regularMarketPrice']
        quantities = df[df['Ticker'] == ticker]['Quantita']
        value = quantities * current_price
        
        portfolio_data.append({
            'Ticker': ticker,
            'TER': ter, 
            'Quantity': quantities.sum(),
            'Current Price': current_price,
            'Value': value.sum()
        })
        
    return pd.DataFrame(portfolio_data)

def _calculate_total_value(portfolio, config, stock_price):
    """Calculate total portfolio value"""
    current_value = portfolio['Value'].sum()
    if stock_price is not None:
        stock_value = round(config['params']['azioni_luxottica'] * stock_price, 0)
        return current_value + stock_value
    else:
        logging.warning("Stock price not available - returning only portfolio value without Luxottica shares")
        return current_value

def _calculate_currency_risk(portfolio, config_portfolio, countries):
    """Calculate currency risk exposure for portfolio.
    
    Args:
        portfolio: Portfolio holdings DataFrame
        config_portfolio: Portfolio configuration
        countries: Country and currency data
        
    Returns:
        DataFrame: Currency risk exposure analysis
    """
    # Calculate normalized weights
    currency_weights, _ = calculate_normalized_weights(config_portfolio, countries)
    
    # Modify weights for hedged ETFs
    for ticker in currency_weights.columns:
        try:
            long_name = Ticker(ticker).quotes[ticker]['longName']
            if "Hedged" in long_name and "EUR" in long_name:
                currency_weights[ticker] = 0
                currency_weights.at["EUR", ticker] = 1
        except KeyError:
            continue
    
    # Convert to long format for merging
    df_weights = currency_weights.reset_index()
    df_weights_long = df_weights.melt(
        id_vars=['currency'],
        var_name='Ticker',
        value_name='Weight'
    )
    
    # Ensure numeric types
    merged = pd.merge(portfolio, df_weights_long, on='Ticker')
    merged['Value'] = pd.to_numeric(merged['Value'], errors='coerce')
    merged['Weight'] = pd.to_numeric(merged['Weight'], errors='coerce')
    
    # Calculate currency exposure
    merged['Currency Exposure'] = merged['Value'] * merged['Weight']
    
    # Calculate risk percentages
    risk = merged.groupby('currency')['Currency Exposure'].sum().reset_index()
    total = portfolio['Value'].sum()
    risk['Risk Percentage'] = risk['Currency Exposure'] / total * 100
    
    return risk.sort_values('Risk Percentage', ascending=False)

def _calculate_country_risk(portfolio, countries):
    """Calculate country risk exposure"""
    countries['percentage'] = countries['percentage'].astype(float)
    merged = pd.merge(portfolio, countries, left_on='Ticker', right_on='symbol')
    merged['Country Exposure'] = merged['Value'] * merged['percentage']
    
    risk = merged.groupby('countries')['Country Exposure'].sum().reset_index()
    total = portfolio['Value'].sum()
    risk['Risk Percentage'] = risk['Country Exposure'] / total
    
    return risk.sort_values('Risk Percentage', ascending=False)

def _calculate_sector_risk(portfolio):
    """Calculate sector risk exposure"""
    sector_weights = get_sector_weightings(portfolio)
    max_weights = calculate_max_sector_weights(sector_weights)
    
    selected_data = portfolio[['Ticker', 'Value']].set_index('Ticker').transpose()
    
    sector_risk = pd.DataFrame()
    for ticker in selected_data.columns:
        # Skip if ticker not in max_weights columns
        if ticker not in max_weights.columns:
            continue
            
        weights = max_weights[['Sector', ticker]]
        value = selected_data[ticker].iloc[0]
        weights['Weighted Value'] = value * weights[ticker]
        sector_risk = pd.concat([sector_risk, weights[['Sector','Weighted Value']]])
    
    sector_risk = sector_risk.groupby('Sector', as_index=False)['Weighted Value'].sum()
    total = portfolio['Value'].sum()
    sector_risk['Percentage'] = sector_risk['Weighted Value'] / total
    
    return sector_risk

def _calculate_holdings_risk(portfolio):
    """Calculate holdings risk exposure"""
    holdings_weights = get_holdings_weightings(portfolio)
    max_weights = calculate_max_holdings_weights(holdings_weights).fillna(0)
    
    selected_data = portfolio[['Ticker', 'Value']].set_index('Ticker').transpose()
    
    holdings_risk = pd.DataFrame()
    for ticker in selected_data.columns:
        try:
            if ticker not in max_weights.columns:
                logging.info(f"Skipping {ticker} - not found in holdings data")
                continue
            weights = max_weights[['Ticker', 'holdingName', ticker]]
            value = selected_data[ticker].iloc[0]
            weights['Weighted Value'] = value * weights[ticker]
            holdings_risk = pd.concat([holdings_risk, 
                                    weights[['Ticker','holdingName','Weighted Value']]])
        except KeyError:
            logging.info(f"Skipping {ticker} - not found in holdings data")
            continue
    
    holdings_risk = holdings_risk.groupby(['Ticker','holdingName'], 
                                      as_index=False)['Weighted Value'].sum()
    total = portfolio['Value'].sum()
    holdings_risk['Percentage'] = holdings_risk['Weighted Value'] / total
    
    return holdings_risk

def _generate_portfolio_trend(df):
    """Generate portfolio trend visualization with correct handling of cumulative investments"""
    df['Value Date'] = pd.to_datetime(df['Value Date']).dt.tz_localize(None)
    df = df.dropna(subset=['Descrizione'])
    
    # Inizializza dataframe vuoti con una struttura temporale comune
    min_date = df['Value Date'].min()
    max_date = pd.Timestamp.now() + pd.Timedelta(days=5)
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    
    # Inizializza dataframe vuoti con la struttura temporale completa
    portfolio_value = pd.DataFrame({'Date': date_range})
    portfolio_value['Value'] = 0.0
    
    cumulative_investments = pd.DataFrame({'Date': date_range})
    cumulative_investments['Cumulative Investment'] = 0.0
    
    # Dizionario per tracciare gli investimenti per data
    investments_by_date = {}
    
    # Calcola direttamente l'investimento cumulativo dalle operazioni
    for _, row in df.iterrows():
        trans_date = row['Value Date']
        quantity = row['Quantita']
        purchase_value = quantity * row['Prezzo']
        
        # Aggiungi questo investimento alla data corrispondente
        if trans_date in investments_by_date:
            investments_by_date[trans_date] += purchase_value
        else:
            investments_by_date[trans_date] = purchase_value
    
    # Applica l'investimento cumulativo al dataframe
    running_total = 0
    for date in sorted(investments_by_date.keys()):
        running_total += investments_by_date[date]
        
        # Trova tutte le date dal giorno dell'investimento in poi
        mask = cumulative_investments['Date'] >= date
        cumulative_investments.loc[mask, 'Cumulative Investment'] = running_total
    
    # Ora calcola il valore del portafoglio usando i dati storici
    for ticker in df['Ticker'].unique():
        try:
            history = yf.Ticker(ticker).history(period="max")
            
            if history.empty:
                print(f"ATTENZIONE: Nessun dato storico trovato per {ticker}")
                continue
                
            history = history.reset_index()
            history['Date'] = history['Date'].dt.tz_localize(None)
            
            # Riempi eventuali buchi nei dati storici
            history = history.set_index('Date')
            history = history.reindex(pd.date_range(start=history.index.min(), end=history.index.max(), freq='D'))
            history = history.reset_index()
            history.rename(columns={'index': 'Date'}, inplace=True)
            
            # Riempi i valori mancanti
            history['Close'] = history['Close'].interpolate(method='linear').ffill().bfill()
            
            # Rimozione spike
            history['rolling_median'] = history['Close'].rolling(window=15, min_periods=1, center=True).median()
            history['deviation'] = abs(history['Close'] - history['rolling_median']) / history['rolling_median']
            spike_threshold = 0.05
            history.loc[history['deviation'] > spike_threshold, 'Close'] = history['rolling_median']
            
            # Smoothing
            history['Close'] = history['Close'].rolling(window=3, min_periods=1, center=True).mean()
            
            # Elabora ogni operazione per questo ticker
            operations = df[df['Ticker'] == ticker]
            for _, row in operations.iterrows():
                trans_date = row['Value Date']
                quantity = row['Quantita']
                
                # Filtra i prezzi dopo la data di transazione
                valid_history = history[history['Date'] >= trans_date].copy()
                
                if not valid_history.empty:
                    # Calcola il valore dell'investimento nel tempo
                    for _, price_row in valid_history.iterrows():
                        date = price_row['Date']
                        close_price = price_row['Close']
                        
                        # Aggiorna il valore del portafoglio per questa data
                        mask = portfolio_value['Date'] == date
                        if mask.any():
                            portfolio_value.loc[mask, 'Value'] += quantity * close_price
                            
        except Exception as e:
            print(f"Errore nell'elaborazione del ticker {ticker}: {str(e)}")
    
    # Rimuovi date future per il valore del portafoglio
    today = pd.Timestamp.now()
    portfolio_value = portfolio_value[portfolio_value['Date'] <= today]
    cumulative_investments = cumulative_investments[cumulative_investments['Date'] <= today]
    
    # Controllo finale per spike nel valore del portafoglio
    portfolio_value['rolling_median'] = portfolio_value['Value'].rolling(window=15, min_periods=1, center=True).median()
    portfolio_value['deviation'] = abs(portfolio_value['Value'] - portfolio_value['rolling_median']) / portfolio_value['rolling_median']
    final_threshold = 0.05
    portfolio_value.loc[portfolio_value['deviation'] > final_threshold, 'Value'] = portfolio_value['rolling_median']
    
    # Applica smoothing finale
    portfolio_value['Value'] = portfolio_value['Value'].rolling(window=5, min_periods=1, center=True).mean()
    
    # Calcolo della percentuale di rendimento
    latest_value = portfolio_value['Value'].iloc[-1]
    latest_investment = cumulative_investments['Cumulative Investment'].iloc[-1]
    
    # Verifica che l'investimento cumulativo non sia zero
    if latest_investment > 0:
        profit_percentage = ((latest_value / latest_investment) - 1) * 100
    else:
        profit_percentage = float('inf')  # O qualsiasi altro valore di default
        print("ATTENZIONE: L'investimento cumulativo è zero!")
    
    # # Debug: stampa i primi e ultimi valori di entrambi i dataframe per diagnostica
    # print("\n--- DEBUG INVESTMENTS ---")
    # print(f"Numero totale di investimenti: {len(investments_by_date)}")
    # print(f"Date di investimento: {sorted(investments_by_date.keys())}")
    # print(f"Valori di investimento: {[investments_by_date[k] for k in sorted(investments_by_date.keys())]}")
    # print(f"Investimento totale: {sum(investments_by_date.values())}")
    
    # print("\n--- DEBUG CUMULATIVE INVESTMENTS ---")
    # print(f"Prime 5 righe: \n{cumulative_investments.head()}")
    # print(f"Ultime 5 righe: \n{cumulative_investments.tail()}")
    # print(f"Range: Min={cumulative_investments['Cumulative Investment'].min()}, Max={cumulative_investments['Cumulative Investment'].max()}")
    
    # Plot migliorato
    plt.figure(figsize=(14, 7))
    plt.plot(portfolio_value['Date'], portfolio_value['Value'], 
             label='Portfolio Value', linewidth=2, color='#1f77b4')
    plt.plot(cumulative_investments['Date'], cumulative_investments['Cumulative Investment'],
             label='Cumulative Investment', linestyle='--', linewidth=2, color='#ff7f0e')
    
    # Output diagnostico
    # print(f"\nPrimo giorno nel dataset: {portfolio_value['Date'].min()}")
    # print(f"Ultimo giorno nel dataset: {portfolio_value['Date'].max()}")
    # print(f"Valore finale del portafoglio: €{latest_value:.2f}")
    # print(f"Investimento cumulativo finale: €{latest_investment:.2f}")
    # if latest_investment > 0:
    #     print(f"Rendimento: {profit_percentage:.2f}%")
    
    # Migliora la visualizzazione
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Value (€)', fontsize=12)
    plt.title("Investment Trend Over Time", fontweight='bold', fontsize=16)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # # Aggiungi annotazione con il valore finale e rendimento
    # if latest_investment > 0:
    #     annotation_text = f"Latest Value: €{latest_value:.2f} ({profit_percentage:+.1f}%)"
    # else:
    #     annotation_text = f"Latest Value: €{latest_value:.2f} (+inf%)"
    
    # plt.annotate(annotation_text, 
    #              xy=(0.02, 0.95), xycoords='axes fraction', 
    #              fontsize=10, 
    #              bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    # Salva l'immagine
    plt.savefig('Trend_investimenti.jpg', transparent=True, dpi=300)
    plt.close()
    
    return portfolio_value, cumulative_investments



def calculate_max_holdings_weights(holdings_weightings):
    """Calculate maximum holdings weights for each ticker and holding name.
    
    Args:
        holdings_weightings (DataFrame): DataFrame containing holdings weightings data
        
    Returns:
        DataFrame: Maximum holdings weights grouped by ticker and holding name
        
    Raises:
        KeyError: If 'Ticker' column is missing from input DataFrame
    """
    if 'Ticker' not in holdings_weightings.columns:
        raise KeyError("Missing required 'Ticker' column in DataFrame")
        
    return holdings_weightings.groupby(['Ticker', 'holdingName']).max().reset_index()

def get_holdings_weightings(portfolio_current):
    """Get holdings weightings for tickers in portfolio."""
    holdings_weightings_list = []
    bond_list = []
    
    for symbol in portfolio_current['Ticker']:
        ticker = Ticker(symbol)
        
        # Skip bond funds
        category_holdings = ticker.fund_category_holdings
        if ('bondPosition' in category_holdings.columns and 
            category_holdings['bondPosition'].iloc[0] > 0.5):
            logging.info(f"Skipping bond fund {symbol}")
            bond_list.append(symbol)
            continue
            
        # Get top holdings with retry mechanism
        retries = 3
        for attempt in range(retries):
            try:
                top_holdings = ticker.fund_top_holdings
                if not top_holdings.empty:
                    holdings = top_holdings.copy()
                    if 'symbol' in holdings.columns:
                        holdings = holdings.rename(columns={'symbol': 'Ticker'})
                    holdings = holdings.reset_index()
                    holdings = holdings.rename(columns={'holdingPercent': symbol})
                    holdings = holdings[['Ticker', 'holdingName', symbol]]
                    holdings_weightings_list.append(holdings)
                else:
                    logging.info(f"No holdings data for {symbol}")
                break  # Exit the retry loop if successful
            except ChunkedEncodingError as e:
                logging.warning(f"ChunkedEncodingError for {symbol}: {str(e)}. Retrying...")
                time.sleep(2)  # Wait before retrying
            except Exception as e:
                logging.error(f"Error fetching holdings for {symbol}: {str(e)}")
                break  # Exit on other exceptions

    if not holdings_weightings_list:
        return None
        
    return pd.concat(holdings_weightings_list, axis=0)

def calculate_max_sector_weights(sector_weightings):
    """Calculate maximum sector weights.
    
    Args:
        sector_weightings (DataFrame): DataFrame with sector weightings
        
    Returns:
        DataFrame: Maximum weights by sector
        
    Raises:
        KeyError: If 'Sector' column is missing
    """
    if 'Sector' not in sector_weightings.columns:
        raise KeyError("Missing required 'Sector' column in DataFrame")
        
    return sector_weightings.groupby('Sector').max().reset_index()

def get_sector_weightings(portfolio_current):
    """Get sector weightings for tickers in portfolio.
    
    Args:
        portfolio_current (DataFrame): Current portfolio holdings
        
    Returns:
        DataFrame: Combined sector weightings across tickers
    """
    sector_weightings_list = []
    bond_list = []
    
    for symbol in portfolio_current['Ticker'].tolist():
        t = Ticker(symbol)
        
        # Skip bond funds
        category_holdings_df = t.fund_category_holdings
        if (('bondPosition' in category_holdings_df.columns and 
            category_holdings_df['bondPosition'].iloc[0] > 0.5) or
            ('otherPosition' in category_holdings_df.columns and 
             category_holdings_df['otherPosition'].iloc[0] > 0.5)):
            logging.info(f"Skipping bond fund or other position fund {symbol}")
            bond_list.append(symbol)
            continue
            
        # Get sector weightings
        sector_weightings_df = t.fund_sector_weightings
        
        if not sector_weightings_df.empty:
            # Create a DataFrame with the sector weightings
            weightings = pd.DataFrame({
                'Sector': sector_weightings_df.index,
                symbol: sector_weightings_df.iloc[:, 0]  # Get first column values
            })
            sector_weightings_list.append(weightings)
        else:
            logging.info(f"No sector data for {symbol}")
            
    if not sector_weightings_list:
        return None
        
    # Merge all sector weightings on 'Sector' column
    result = sector_weightings_list[0]
    for df in sector_weightings_list[1:]:
        result = pd.merge(result, df, on='Sector', how='outer')
        
    return result.fillna(0)

def calculate_total_return(initial_value, current_value):
    """Calculate total return and return percentage.
    
    Args:
        initial_value (float): Initial investment value
        current_value (float): Current investment value
        
    Returns:
        tuple: (total_return, total_return_percentage)
    """
    total_return = current_value - initial_value
    total_return_percentage = (total_return / initial_value) * 100
    return total_return, total_return_percentage

def calculate_similarity(s1, s2):
    """Calculate string similarity using fuzzy matching.
    
    Args:
        s1 (str): First string
        s2 (str): Second string
        
    Returns:
        int: Similarity score between strings
    """
    return fuzz.token_sort_ratio(s1, s2)

def calculate_current_portfolio_value(df):
    """Calculate current portfolio value.
    
    Args:
        df (DataFrame): Portfolio holdings DataFrame
        
    Returns:
        float: Current total portfolio value
    """
    current_value = 0
    for ticker in df['Ticker'].unique():
        price = Ticker(ticker).quotes[ticker]['regularMarketPrice']
        quantity = df[df['Ticker'] == ticker]['Quantita'].sum()
        current_value += price * quantity
    return current_value

def calculate_currency_weights(config, df_currency_weights):
    """Calculate currency weights for portfolio.
    
    Args:
        config (dict): Portfolio configuration
        df_currency_weights (DataFrame): Currency weights DataFrame
        
    Returns:
        DataFrame: Updated currency weights
    """
    for ticker in df_currency_weights.columns:
        try:
            long_name = Ticker(ticker).quotes[ticker]['longName']
            if "Hedged" in long_name and "EUR" in long_name:
                df_currency_weights[ticker] = 0
                df_currency_weights.at["EUR", ticker] = 1
        except KeyError:
            continue
            
    return df_currency_weights

def calculate_normalized_weights(config, countries):
    """Calculate normalized currency and country weights for portfolio ETFs.
    
    Args:
        config: Portfolio configuration containing ETF info
        countries: DataFrame with country and currency data
        
    Returns:
        tuple: (currency_weights, country_weights) DataFrames
    """
    currency_weights = {}
    country_weights = {}

    for etf_info in config['etf']:
        # Update this section to handle dictionary format
        ticker_symbol = etf_info['Ticker'] if isinstance(etf_info, dict) else etf_info[0]
        
        ticker_countries = countries[countries['symbol'] == ticker_symbol]

        # Convert percentages to decimals
        ticker_countries['percentage'] = pd.to_numeric(
            ticker_countries['percentage'].str.strip()
        ) / 100

        # Calculate normalized weights
        currency_weights[ticker_symbol] = ticker_countries.groupby('currency')['percentage'].sum()
        country_weights[ticker_symbol] = ticker_countries.groupby('countries')['percentage'].sum()

    return pd.DataFrame(currency_weights), pd.DataFrame(country_weights)



    
def investimenti_ad_ora(fineco_investimento, conto_deposito_investimento, config):
    """Calculate current investment metrics.
    
    Args:
        fineco_investimento (DataFrame): Investment transaction data
        config (dict): Configuration parameters
        
    Returns:
        tuple: (capitale_investito_Fineco, essilorluxottica_price, diff_in_months_investimento)
            - capitale_investito_Fineco (float): Total invested capital
            - essilorluxottica_price (float): Current EssilorLuxottica stock price
            - diff_in_months_investimento (int): Months until investment tranche end
    """
    # Get EssilorLuxottica stock price
    essilorluxottica_price = get_current_stock_price("1EL.MI")
    if essilorluxottica_price is not None:
        logging.info(f"Current stock price of EssilorLuxottica (1EL.MI): €{essilorluxottica_price:.2f}")
    else:
        logging.info("Unable to retrieve current stock price.")

    # Calculate months until investment tranche end
    fineco_investimento = fineco_investimento.copy()
    fineco_investimento['ds'] = pd.to_datetime(fineco_investimento['Date'], format='%Y-%m-%d')
    
    data_ultima_tranche = fineco_investimento['ds'].max()
    dt_fine_tranche_investimento = pd.to_datetime(
        config['params']['data_fine_PAC_iniziale'], 
        format='%Y-%m-%d'
    )
    
    diff_in_months_investimento = int(
        (dt_fine_tranche_investimento - data_ultima_tranche).days / 30.44
    )

    # Calculate total invested capital
    capitale_investito_Fineco = fineco_investimento['Importo'].sum()

    conto_deposito_investito = conto_deposito_investimento['Importo'].sum()
    
    return capitale_investito_Fineco, conto_deposito_investito, essilorluxottica_price, diff_in_months_investimento, 




def get_current_stock_price(ticker):
    """Get the most recent closing price for a given stock ticker using historical data.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        float: Most recent closing price if successful, None if error occurs
        
    Example:
        >>> price = get_current_stock_price("AAPL")
        >>> print(price)
        150.23
    """
    try:
        # Create Ticker object and get historical data
        stock = yf.Ticker(ticker)
        # Get data for the last month to ensure we have recent data
        history = stock.history(period='1mo')
        
        # Return most recent closing price
        if not history.empty:
            latest_price = history['Close'].iloc[-1]
            logging.info(f"Retrieved latest price for {ticker}: {latest_price}")
            return latest_price
        else:
            logging.warning(f"No historical data found for {ticker}")
            return None
            
    except Exception as e:
        logging.error(f"Error fetching price for {ticker}: {str(e)}")
        return None



def analisi_liquidita(tutto, liquidita_31_giu_22, config):
    """Calculate current and projected account liquidity.
    
    Args:
        tutto (DataFrame): Transaction data
        liquidita_31_giu_22 (float): Account balance as of June 31, 2022
        config (dict): Configuration parameters
        
    Returns:
        float: Total projected savings (initial balance + accumulated savings)
    """
    # Convert dates to datetime
    tutto['ds'] = pd.to_datetime(tutto['ds'], format='%Y-%m-%d')
    
    # Filter transactions after June 2022
    tutto_post_giu = tutto[tutto['ds'] >= config['params']['data_giu']]
    
    # Calculate total savings since June 2022
    risparmiato = tutto_post_giu['Valore'].sum()
    
    # Return total projected savings
    return liquidita_31_giu_22 + risparmiato



def dati_sintesi(reddito, spese, columns_to_drop, config):
    """Calculate average monthly financial metrics."""
    # # Convert columns_to_drop to list if it's a dictionary
    # if isinstance(columns_to_drop, dict):
    #     columns_to_drop = list(columns_to_drop.keys())
    # elif not isinstance(columns_to_drop, list):
    #     columns_to_drop = []

    # Calculate Federico's average salary
    last_12_months_mean_Fede = _calculate_salary_mean(
        reddito, 'Federico', columns_to_drop)
    
    # Calculate Anna's average salary  
    last_12_months_mean_Anna = _calculate_salary_mean(
        reddito, 'Anna', columns_to_drop)
    
    # Calculate average child benefit
    Valore_medio_assegno_unico = _calculate_child_benefit_mean(
        reddito, config['params']['mese_senza_retta_scolastica'])
        
    # Calculate average child expenses
    values_bimbo_mean = _calculate_child_expenses_mean(
        spese, config['params']['mese_senza_retta_scolastica'])
        
    return (last_12_months_mean_Fede, last_12_months_mean_Anna, 
            Valore_medio_assegno_unico, values_bimbo_mean)

def _calculate_salary_mean(reddito, account, columns_to_drop):
    """Calculate average monthly salary for an account."""
    salary_data = reddito[(reddito['Conto']==account) & 
                         (reddito['Categoria']=='Salary')].copy()
    
    salary_data['YearMonth'] = pd.to_datetime(salary_data['YearMonth'], 
                                            format='%Y-%m')
    salary_data.set_index('YearMonth', inplace=True)
    salary_data.sort_index(inplace=True)
    
    columns_to_drop_existing = [col for col in columns_to_drop 
                              if col in salary_data.columns]
    
    return salary_data.drop(columns_to_drop_existing).tail(12)['Importo'].mean()

def _calculate_child_benefit_mean(reddito, excluded_month):
    """Calculate average monthly child benefit."""
    assegno_unico = (reddito[reddito['SottoCategoria'] == 'Assegno Unico']
        .groupby(['YearMonth'], as_index=False)['Importo']
        .sum()
        .sort_values('YearMonth', ascending=True)
        .tail(6))
    
    assegno_unico['YearMonth'] = pd.to_datetime(assegno_unico['YearMonth'])
    
    assegno_unico_filtered = assegno_unico[
        ~assegno_unico['YearMonth'].dt.strftime('%m').eq(excluded_month)]
        
    return assegno_unico['Importo'].mean()

def _calculate_child_expenses_mean(spese, excluded_month):
    """Calculate average monthly child expenses."""
    values_bimbo = (spese[spese['Categoria']=='Bimbo']
        [['YearMonth','Importo']]
        .groupby(['YearMonth'], as_index=False)
        .sum()
        .sort_values('YearMonth', ascending=True)
        .tail(6))
    
    values_bimbo['YearMonth'] = pd.to_datetime(values_bimbo['YearMonth'])
    
    values_bimbo_filtered = values_bimbo[
        ~values_bimbo['YearMonth'].dt.strftime('%m').eq(excluded_month)]
        
    return values_bimbo_filtered['Importo'].mean()


def aggiunta_info_recap(entr, mesi_entrate_anomale, usc):
    """Calculate average monthly income, expenses and savings."""
    # Process income data
    entr = _prepare_monthly_data(entr.reset_index())
    
    # Handle anomalous months
    if isinstance(mesi_entrate_anomale, dict):
        # Convert dictionary dates to datetime for comparison
        dates_to_exclude = []
        for date_str in mesi_entrate_anomale.keys():
            try:
                date = pd.to_datetime(date_str)
                dates_to_exclude.append(date)
            except:
                continue
                
        # Filter out anomalous months
        entr_filtered = entr[~entr.index.isin(dates_to_exclude)]
    else:
        # If not a dictionary, assume it's already a list of dates to exclude
        entr_filtered = entr[~entr.index.isin(mesi_entrate_anomale)]
    
    # Calculate averages
    average_monthly_income = entr_filtered.tail(18)['Income'].mean()
    
    # Process expenses data
    usc = _prepare_monthly_data(usc.reset_index())
    average_monthly_expenses = usc['Expense'].mean()
    
    # Calculate average monthly savings
    average_monthly_savings = average_monthly_income - average_monthly_expenses
    
    return average_monthly_income, average_monthly_expenses, average_monthly_savings

def _prepare_monthly_data(df):
    """Helper function to prepare monthly data.
    
    Args:
        df (DataFrame): DataFrame to process
        
    Returns:
        DataFrame: Processed DataFrame with YearMonth as index
    """
    df['YearMonth'] = pd.to_datetime(df['YearMonth'], format='%Y-%m')
    df.set_index('YearMonth', inplace=True)
    df.sort_index(inplace=True)
    return df



def manipolazione_table(tutto):
    """Process transaction data to calculate monthly income, expenses and savings.
    
    Args:
        tutto (DataFrame): Raw transaction data with columns YearMonth, Tipo, Importo
        
    Returns:
        tuple: (allo, entr, usc)
            - allo (DataFrame): Combined monthly income/expenses with savings
            - entr (DataFrame): Monthly income totals 
            - usc (DataFrame): Monthly expense totals
    """
    # Convert YearMonth to datetime and filter last 18 months
    tutto['y_m_d'] = pd.to_datetime(tutto['YearMonth'], format='%Y-%m')
    date_cutoff = datetime.now() - timedelta(days=19*365/12)
    tutto = tutto[tutto['y_m_d'] > date_cutoff]
    
    # Calculate monthly income totals
    entr = (tutto[tutto['Tipo'] == 'Income']
            [['YearMonth', 'Importo']]
            .groupby('YearMonth')
            .sum()
            .rename(columns={'Importo': 'Income'}))
    
    # Calculate monthly expense totals  
    usc = (tutto[tutto['Tipo'] == 'Expense']
           [['YearMonth', 'Importo']]
           .groupby('YearMonth')
           .sum()
           .rename(columns={'Importo': 'Expense'}))
    
    # Merge income and expenses, calculate savings
    allo = pd.merge(entr, usc, on='YearMonth', how='outer').fillna(0)
    allo['Risparmiato'] = allo['Income'] - allo['Expense']
    
    return allo, entr, usc


# def get_table():
#     data = table_export
#     return data



def bar_chart(tutto):
    """Create bar chart comparing last month vs 12-month average expenses by category.
    
    Args:
        tutto (DataFrame): Transaction data with columns YearMonth, Tipo, Categoria, Importo
        
    Returns:
        None: Saves plot to histogram_last_month.jpg and displays it
    """
    # Get last month and last 12 months data
    last_month = tutto['YearMonth'].max()
    last_12_months = tutto[['YearMonth']].drop_duplicates().tail(12)['YearMonth'].unique()
    
    # Calculate averages by category
    def get_category_averages(data, time_filter, col_name):
        filtered = data[(data['Tipo'] == 'Expense') & time_filter].groupby(
            ['Categoria', 'YearMonth'], as_index=False)['Importo'].sum()
        return pd.pivot_table(filtered, values='Importo', index='Categoria', 
                            aggfunc=np.mean).reset_index().rename(columns={'Importo': col_name})
    
    hist_avg = get_category_averages(tutto, tutto['YearMonth'].isin(last_12_months), 'hist')
    last_month_avg = get_category_averages(tutto, tutto['YearMonth'] == last_month, 'last_month')
    
    # Merge the averages
    plot_data = pd.merge(hist_avg, last_month_avg, on='Categoria', how='outer').fillna(0)
    
    # Create plot
    fig, ax = plt.subplots()
    bar_width = 0.35
    indices = np.arange(len(plot_data['Categoria']))
    
    ax.bar(indices - bar_width/2, plot_data['hist'], bar_width, label='Storico 12 mesi')
    ax.bar(indices + bar_width/2, plot_data['last_month'], bar_width, label='Ultimo mese')
    
    # Customize plot
    ax.set_xlabel('Categoria')
    ax.set_ylabel('Importo')
    ax.set_title('Andamento ultimo mese per categoria', fontweight='bold')
    ax.set_xticks(indices)
    ax.set_xticklabels(plot_data['Categoria'], rotation=45, ha='right')
    ax.legend()
    
    plt.tight_layout()
    plt.savefig('histogram_last_month.jpg', transparent=True)
    plt.close()



def time_series_graph(time_series, config):
    """Create time series plot of account balance over time.
    
    Args:
        time_series (DataFrame): Time series data with 'ds' and 'y' columns
        config (dict): Configuration parameters
        
    Returns:
        None: Displays plot and prints confirmation message
    """
    # Convert the 'ds' column to datetime.date if it's currently a string
    time_series['ds'] = pd.to_datetime(time_series['ds']).dt.date

    # Filter data and adjust values
    cutoff_date = config['params']['data_giu']
    cutoff_date = dt.datetime.strptime(cutoff_date, '%Y-%m-%d').date()  # Ensure cutoff_date is a datetime.date object

    filtered_series = time_series[time_series['ds'] < cutoff_date]

    last_value = filtered_series.tail(1)['y'].sum()
    adjustment = config['params']['liquidita_31_giu_22'] - last_value

    # Process time series data
    adjusted_series = time_series.copy()
    adjusted_series['y'] += adjustment
    adjusted_series = adjusted_series[['ds', 'y']]
    adjusted_series['ds'] = pd.to_datetime(adjusted_series['ds'])
    adjusted_series.set_index('ds', inplace=True)
    final_series = adjusted_series['y']
    
    # Create plot
    plt.xlabel("Data")
    plt.ylabel("Euro") 
    plt.title("Andamento Risparmi",fontweight='bold')
    plt.plot(final_series)
    plt.savefig('Andamento_risparmi.jpg')
    plt.close()
    
    logging.info("Grafico creato")



def update_category(row):
    """Update category based on transaction type and subcategory.
    
    Args:
        row (Series): DataFrame row containing transaction data
        
    Returns:
        str: Updated category value
    """
    if row['Tipo'] == 'Expense' and row['Categoria'] == 'Food' and row['SottoCategoria'] in ['Settimanale ','Settimanale']:
        return 'Settimanale'
    else:
        return row['Categoria']
    

def process_backup(backup_zip):
    """Esegue l'intero processo di estrazione e conversione dati per un file ZIP specifico."""
    # Crea la cartella di destinazione con lo stesso nome del file ZIP (senza estensione)
    backup_folder = backup_zip.replace(".zip", "")
    
    # Estrai il contenuto
    with zipfile.ZipFile(backup_zip, 'r') as zip_ref:
        zip_ref.extractall(backup_folder)
    
    print(f"File {backup_zip} estratto in {backup_folder}")

    # Percorso del database SQLite
    db_path = os.path.join(backup_folder, "GetRichV1.sqlite")
    
    # Crea la cartella 'extraction' se non esiste
    os.makedirs("extraction", exist_ok=True)

    # Connessione al database SQLite
    conn = sqlite3.connect(db_path)
    
    # Ottieni tutte le tabelle presenti nel database
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql(query, conn)

    # Esporta ogni tabella in un file CSV
    for table_name in tables["name"]:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        csv_file = f"extraction/{table_name}.csv"
        df.to_csv(csv_file, index=False)
        print(f"Tabella '{table_name}' esportata in {csv_file}")
    
    # Chiudi la connessione
    conn.close()

    # Caricamento dei dati dalle tabelle esportate
    transactions = pd.read_csv('extraction/ZTRANSACTION.csv')

    # transactions = transactions[(transactions['ZRECURRING'].isnull())]

    transactions = transactions[transactions['ZISACTIVE']==1]
    
    pd.set_option("display.max_columns", 50)

    # Rimozione delle colonne non necessarie
    transactions = transactions.drop(columns=[
        'ZLOCATION', 'ZISEXCLUDEDFROMCALCULATIONS', 'ZOWNER', 'ZTRANSACTIONNAME',
        'ZTRANSACTIONDESCRIPTION', 'ZEXCHANGERATE', 'ZTRANSFER', 'ZTRANSACTIONACCOUNT',
        'ZRECURRING', 'ZPAYEE', 'ZCHILDRECURRINGS', 'ZISTRANSFER', 'ZISRECURRING',
        'Z_ENT', 'Z_OPT', 'ZISACTIVE', 'ZISCLEARED'
    ], errors='ignore')
    
    # Caricamento delle altre tabelle
    account_name = pd.read_csv('extraction/ZACCOUNT.csv')['ZACCOUNTNAME'].max()
    category = pd.read_csv('extraction/ZTRANSACTIONCATEGORY.csv')
    subcategory = pd.read_csv('extraction/ZTRANSACTIONSUBCATEGORY.csv')
    
    # Rinominare le colonne e unire i dati
    category = category[['Z_PK', 'ZCATEGORYNAME']].rename(columns={'Z_PK': 'ZTRANSACTIONCATEGORY'})
    subcategory = subcategory[['Z_PK', 'ZSUBCATEGORYNAME']].rename(columns={'Z_PK': 'ZTRANSACTIONSUBCATEGORY'})
    
    transactions = pd.merge(transactions, category, on='ZTRANSACTIONCATEGORY', how='left')
    transactions = pd.merge(transactions, subcategory, on='ZTRANSACTIONSUBCATEGORY', how='left')
    

    # Funzione per convertire la data in formato ISO 8601
    def convert_date(row):
        try:
            # Combina il giorno, mese e anno per creare un oggetto datetime
            date_str = f"{int(row['ZTRANSACTIONYEAR'])}-{int(row['ZTRANSACTIONMONTH']):02d}-{int(row['ZTRANSACTIONDAY']):02d}T00:00:00Z"
            return date_str
        except Exception as e:
            print(f"Errore nella conversione della data per la transazione: {e}")
            return None

    # Applica la funzione alla colonna
    transactions['Data (ISO 8601)'] = transactions.apply(convert_date, axis=1)

    # Aggiungere il nome del conto
    transactions['Conto'] = account_name
    
    # Rinomina colonne per una migliore leggibilità
    transactions = transactions.rename(columns={
        'ZCATEGORYNAME': 'Categoria',
        'ZSUBCATEGORYNAME': 'SottoCategoria',
        'ZTRANSACTIONAMOUNT': 'Importo',
        'ZCURRENCYCODE': 'Valuta',
        'ZTRANSACTIONTYPE': 'Tipo'
    })
    
    transactions['Importo'] = transactions['Importo'].round(2)
    
    # Aggiungere una colonna con l'importo convertito
    transactions['Importo convertito (EUR)'] = transactions['Importo']
    
    # Seleziona le colonne finali
    transactions = transactions[['Data (ISO 8601)', 'Conto', 'Categoria', 'SottoCategoria', 'Importo', 'Valuta', 'Importo convertito (EUR)', 'Tipo']]
    
    # Salva il file CSV finale
    transactions.to_csv(f"data/transactions_{account_name}.csv", index=False)
    
    print(f"File data/transactions_{account_name}.csv creato con successo!")
    
    # Verifica se il file è stato creato
    if os.path.exists(f"data/transactions_{account_name}.csv"):
        logging.info(f"File creato con successo: data/transactions_{account_name}.csv")
    else:
        logging.error(f"Errore nella creazione del file: data/transactions_{account_name}.csv")
    
    # Eliminazione delle cartelle create
    shutil.rmtree(backup_folder)
    shutil.rmtree("extraction")
    
    print(f"Cartelle {backup_folder} e extraction eliminate.")

def read_latest_files():
    """Read and combine the two most recent MoneyCoach export files.
    
    Returns:
        DataFrame: Combined data from latest export files
    """
    # Modifica il percorso se necessario
    data_dir = os.path.join(os.path.dirname(__file__), "data")  # Assicurati che 'data' sia nella stessa directory di utils.py
    file_pattern = os.path.join(data_dir, "transactions_*.csv")
    
    # Get list of matching files sorted by creation time
    files = sorted(glob.glob(file_pattern), 
                  key=os.path.getctime, 
                  reverse=True)[:2]
    
    # Log the found files
    logging.info(f"Found files: {files}")

    # Check if any files were found
    if not files:
        logging.error("No transaction files found matching the pattern.")
        raise FileNotFoundError("No transaction files found.")

    # Read and combine files
    dfs = [pd.read_csv(f) for f in files]
    
    # Check if any DataFrames were created
    if not dfs:
        logging.error("No data found in the transaction files.")
        raise ValueError("No data to concatenate from the transaction files.")
    
    return pd.concat(dfs, ignore_index=True)

def transform_dates(df):
    """Transform and clean transaction data.
    
    Args:
        df (DataFrame): Raw transaction data
        
    Returns:
        DataFrame: Cleaned and transformed data
    """
    # Drop unused columns
    drop_cols = ['Importo convertito (EUR)']
    df = df.drop(drop_cols, axis=1)
    
    # Clean and format data
    df = df.sort_values('Data (ISO 8601)')
    
    # Convert 'Data (ISO 8601)' to datetime
    df['Date'] = pd.to_datetime(df['Data (ISO 8601)'].str[:10], format='%Y-%m-%d')
    df['Date'] = df['Date'].dt.date  # Convert to date only (no time)

    today_date = dt.datetime.today()
    first_day_month = dt.date(day=1, month=today_date.month, year=today_date.year)
    
    # Convert 'cutoff_date' to date format (if it is datetime)
    cutoff_date = dt.datetime.combine(first_day_month, dt.datetime.min.time()).date()

    # Filter by date (both are 'datetime.date' objects)
    df = df[df['Date'] < cutoff_date]

    
    # Calculate values
    df['Segno'] = df['Tipo'].map({'Income': 1, 'Expense': -1})
    df['Valore'] = df['Segno'] * df['Importo']

    return df


def transform_transactions(df):
    """Further transform transaction data for analysis.
    
    Args:
        df (DataFrame): Transformed transaction data
        
    Returns:
        DataFrame: Final processed data
    """
    # Filter out specific transactions
    mask = ~((df['Tipo'] == "Expense") & 
             (df['Categoria'] == "Finance") & 
             (df['SottoCategoria'].isin(["Fineco", "Conto deposito"])))
    df = df[mask]
    
    # Update categories
    df['Categoria'] = df.apply(update_category, axis=1)
    
    # Sort and calculate running totals
    df = df.sort_values("Date")
    df['y'] = df['Valore'].cumsum()
    df['ds'] = df['Date']
    
    # Create final DataFrame
    return df.drop(['Date'], axis=1)

def calculate_future_predictions(all_data, config, capitale_investito_Fineco, controvalore_investimento, diff_in_months_investimento):
    """
    Calculate future investment predictions including capital growth and returns.
    
    Args:
        all_data: Dictionary with historical transaction data
        config: Configuration dictionary 
        capitale_investito_Fineco: Current invested capital
        controvalore_investimento: Current investment value
        diff_in_months_investimento: Months remaining in investment period
        
    Returns:
        DataFrame: Final summary of future predictions
    """
    # Debug: Print keys in all_data
    logging.info("Keys in all_data:", all_data.keys())

    # Check if 'income' and 'expenses' are in all_data
    if 'income' not in all_data or 'expenses' not in all_data:
        raise KeyError("Missing 'income' or 'expenses' in all_data")

    # Initialize parameters
    annual_interest = config['params']['interesse_lordo_Portafoglio_azionario'] / 100
    monthly_interest = (1 + annual_interest) ** (1 / 12) - 1
    investimento_ricorrente_Fineco = config['params']['importo_PAC_iniziale']
    investimento_annuale_ribilanciamento_stimato = config['params']['investimento_annuale_ribilanciamento_stimato']
    mese_investimento_annuale = config['params']['mese_ribilanciamento']

    # Calculate future savings projections
    future_proj = FutureProjections(all_data['income'], all_data['expenses'], all_data['expense_details'], config, inflation_rate)
    future_proj.calculate_future()
    future_proj.add_tax_payment()

    # Get future savings data
    savings = all_data['current_savings']
    merged = future_proj.merged
    merged['Liquidita_ipotetica'] = merged['Rolling_Spesa_Mean'] * config['params']['mesi_di_spese_correnti']
    merged['Conto_deposito'] = merged['Rolling_Spesa_Mean'] * config['params']['mesi_di_spese_impreviste']
    futuro = merged[['YearMonth', 'Totale', 'Liquidita_ipotetica', 'Conto_deposito']]
    futuro['Future_savings_sum'] = savings + futuro['Totale'].cumsum()
    futuro.drop('Totale', axis=1, inplace=True)

    # Initialize DataFrame for future investments
    investimento_futuro = pd.DataFrame(columns=['YearMonth', 'Capitale_Investito'])

    # Calculate initial values for first month
    listone = list(futuro['YearMonth'].values)
    if diff_in_months_investimento > 0:
        initial_capital = capitale_investito_Fineco + investimento_ricorrente_Fineco
    else:
        initial_capital = capitale_investito_Fineco

    investimento_futuro.loc[0] = [listone[0], initial_capital]

    # Calculate values for remaining months
    for i, date in enumerate(listone[1:], 1):
        prev_capital = investimento_futuro.loc[i-1, 'Capitale_Investito']
        month_value = pd.to_datetime(date).strftime('%m')
        
        if i < diff_in_months_investimento:
            new_capital = prev_capital + investimento_ricorrente_Fineco
        else:
            new_capital = prev_capital + (investimento_annuale_ribilanciamento_stimato if month_value == mese_investimento_annuale else 0)
        
        investimento_futuro.loc[i] = [date, new_capital]

    # Merge with future savings data
    investimento_futuro = pd.merge(investimento_futuro, futuro, on='YearMonth')
    investimento_futuro['Liquidita_Reale'] = investimento_futuro['Future_savings_sum'] - investimento_futuro['Liquidita_ipotetica'] - investimento_futuro['Conto_deposito'] - investimento_futuro['Capitale_Investito']

    # Initialize DataFrame for future investment values
    investimento_futuro_bis = pd.DataFrame(columns=['YearMonth', 'Valore_investimento_Fineco', 'Capitale_Investito'])

    # Calculate initial investment value
    if diff_in_months_investimento > 0:
        initial_value = controvalore_investimento * (1 + monthly_interest) + investimento_ricorrente_Fineco
    else:
        initial_value = controvalore_investimento * (1 + monthly_interest)

    investimento_futuro_bis.loc[0] = [listone[0], initial_value, capitale_investito_Fineco]

    # Calculate investment values for remaining months
    for i, date in enumerate(listone[1:], 1):
        prev_value = investimento_futuro_bis.loc[i-1, 'Valore_investimento_Fineco']
        prev_capital = investimento_futuro_bis.loc[i-1, 'Capitale_Investito']
        month_value = pd.to_datetime(date).strftime('%m')
        liquidity = investimento_futuro.loc[i, 'Liquidita_Reale']
        
        base_value = prev_value * (1 + monthly_interest)
        if i < diff_in_months_investimento:
            new_value = base_value + investimento_ricorrente_Fineco
            new_capital = prev_capital + investimento_ricorrente_Fineco
        else:
            if month_value == investment_month and liquidity > 0:
                new_value = base_value + investimento_annuale_ribilanciamento_stimato
                new_capital = prev_capital + investimento_annuale_ribilanciamento_stimato
            else:
                new_value = base_value
                new_capital = prev_capital
        
        investimento_futuro_bis.loc[i] = [date, new_value, new_capital]

    # Create final summary
    summary = pd.merge(futuro[['YearMonth', 'Future_savings_sum']], investimento_futuro_bis, on='YearMonth')
    summary['Patrimonio_futuro_lordo'] = summary['Future_savings_sum'] - summary['Capitale_Investito'] + summary['Valore_investimento_Fineco']
    summary['Patrimonio_other'] = summary['Future_savings_sum'] - summary['Capitale_Investito']

    # Format the final summary
    summary['YearMonth'] = summary['YearMonth'].dt.strftime('%Y-%m')
    return summary
