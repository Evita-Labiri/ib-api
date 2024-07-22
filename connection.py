import os
import queue
from datetime import datetime, timedelta
import pytz

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
import dash
from dash import dcc, html
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import ta



class TradingApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = []
        self.data_ready_queue = queue.Queue()
        self.lock = threading.Lock()
        self.historical_data_downloaded = False  # Flag to check if historical data is downloaded
        self.ohlcv_data = {}
        # self.user_interval = '5T'  # Προκαθορισμένο διάστημα
        self.last_update_time = None


        self.db_engine = self.create_engine()
        if self.db_engine:
            self.Session = sessionmaker(bind=self.db_engine)
            self.db_session = self.Session()
        else:
            self.Session = None
            self.db_session = None


    def create_engine(self):
        try:
            connection_string = f"mysql+mysqlconnector://{os.getenv('STOCKDATADB_UN')}:{os.getenv('STOCKDATADB_PASS')}@localhost/stockdatadb"
            engine = create_engine(connection_string, connect_args={'connect_timeout': 28800})
            print("Successfully connected to the database with SQLAlchemy")
            return engine
        except Exception as e:
            print(f"Error while connecting to the database with SQLAlchemy: {e}")
            return None

    def ensure_connection(self):
        try:
            if self.db_engine is None:
                print("Database engine is not available, reconnecting...")
                self.db_engine = self.create_engine()
                if self.db_engine:
                    self.Session = sessionmaker(bind=self.db_engine)
                    self.db_session = self.Session()

            if self.db_session is None:
                print("Database session is not available, creating a new session...")
                self.db_session = self.Session()

            # Check if the session is active
            if self.db_session and not self.db_session.is_active:
                print("Database session is not active, creating a new session...")
                self.db_session = self.Session()
            else:
                print("Database session is active and connected.")
        except SQLAlchemyError as e:
            print(f"Error ensuring connection: {e}")
            if self.db_session:
                self.db_session.rollback()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson = ""):
        print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId

    def historicalData(self, reqId, bar):
        print("Requesting data...")
        print(
            f'Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}')
        try:
            ny_tz = pytz.timezone('America/New_York')
            date = None  # Αρχικοποίηση της μεταβλητής date
            if reqId == 1:  # Minute data
                date_str, time_str, tz_str = bar.date.split()
                date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
                date = ny_tz.localize(date).replace(tzinfo=None)
            elif reqId == 2:  # Daily data
                date_str = bar.date
                date = datetime.strptime(date_str, '%Y%m%d')
                date = ny_tz.localize(date).replace(tzinfo=None)
            self.data.append([date, bar.open, bar.high, bar.low, bar.close, bar.volume])
            if reqId == 1:
                self.insert_data_to_minute_table('minute_data', date, bar.open, bar.high, bar.low, bar.close,
                                                 bar.volume)
            elif reqId == 2:
                self.insert_data_to_daily_table('daily_data', date, bar.open, bar.high, bar.low, bar.close, bar.volume)
        except ValueError as e:
            print(f"Error converting date: {e}")

    def historicalDataEnd(self, reqId, start, end):
        print("Historical data download complete")
        self.data_ready_queue.put(self.data)

    def insert_data_to_db(self, df, table_name):
        for index, row in df.iterrows():
            self.ensure_connection()
            try:
                query = text(f"""
                                  INSERT INTO {table_name} (ticker, datetime, open, high, low, close, volume)
                                  VALUES (:ticker, :datetime, :open, :high, :low, :close, :volume)
                              """)
                self.db_session.execute(query, {
                    'ticker': row['ticker'],
                    'datetime': row['datetime'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
                self.db_session.commit()
            except SQLAlchemyError as e:
                print(f"Error inserting data into {table_name}: {e}")
                self.db_session.rollback()

    def insert_data_to_minute_table(self, table_name, date, open, high, low, close, volume):
        ticker = "AAPL"
        try:
            self.ensure_connection()
            query = text(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = :ticker AND date_time = :date_time")
            result = self.db_session.execute(query, {
                'ticker': ticker,
                'date_time': date
            })

            row = result.fetchone()
            if row and row[0] == 0:
                print(f"Inserting minute data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                insert_query = text(f"""
                                INSERT INTO {table_name} (ticker, date_time, open, high, low, close, volume)
                                VALUES (:ticker, :date_time, :open, :high, :low, :close, :volume)
                            """)
                self.db_session.execute(insert_query, {
                    'ticker': ticker,
                    'date_time': date,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume
                })
                self.db_session.commit()
                print("Data inserted")
            else:
                print(f"Duplicate minute data found for {ticker} at {date}, skipping insertion.")
        except SQLAlchemyError as e:
            print(f"Error inserting minute data into {table_name}: {e}")
            self.db_session.rollback()

    def insert_data_to_daily_table(self, table_name, date, open, high, low, close, volume):
        ticker = "AAPL"
        try:
            self.ensure_connection()
            query = text(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = :ticker AND date = :date")
            result = self.db_session.execute(query, {
                'ticker': ticker,
                'date': date
            })

            row = result.fetchone()
            if row and row[0] == 0:
                print(f"Inserting daily data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                insert_query = text(f"""
                     INSERT INTO {table_name} (ticker, date, open, high, low, close, volume)
                     VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                 """)
                self.db_session.execute(insert_query, {
                    'ticker': ticker,
                    'date': date,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume
                })
                self.db_session.commit()
                print("Data inserted")
            else:
                print(f"Duplicate daily data found for {ticker} at {date}, skipping insertion.")
        except SQLAlchemyError as e:
            print(f"Error inserting daily data into {table_name}: {e}")
            self.db_session.rollback()
    def tickPrice(self, reqId, tickType, price, attrib):
        print("Tick Price called now: ")
        # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')
        if tickType == 4:  # Last
            # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
            with self.lock:
                if reqId not in self.ohlcv_data:
                    self.ohlcv_data[reqId] = {
                        'timestamp': timestamp,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': 0,

                    }
                else:
                    self.ohlcv_data[reqId]['close'] = price
                    if price > self.ohlcv_data[reqId]['high']:
                        self.ohlcv_data[reqId]['high'] = price
                    if price < self.ohlcv_data[reqId]['low']:
                        self.ohlcv_data[reqId]['low'] = price

                self.real_time_data.append([
                    timestamp,
                    self.ohlcv_data[reqId]['open'],
                    self.ohlcv_data[reqId]['high'],
                    self.ohlcv_data[reqId]['low'],
                    self.ohlcv_data[reqId]['close'],
                    float(self.ohlcv_data[reqId]['volume'])
                ])
                self.data_ready_queue.put(self.real_time_data[-1])
                # self.update_plot()

    def tickSize(self, reqId, tickType, size):
        print("Tick Size called now: ")
        # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}, Timestamp: {timestamp}')

        if tickType == 5:  # Volume
            with (self.lock):
                if reqId in self.ohlcv_data:
                    self.ohlcv_data[reqId]['volume'] = size
                else:
                    self.ohlcv_data[reqId] = {
                        'timestamp': timestamp,
                        'open': 0,
                        'high': 0,
                        'low': 0,
                        'close': 0,
                        'volume': size
                    }

                # self.update_plot()

    def fetch_table_columns(self, table_name):
        query = f"SHOW COLUMNS FROM {table_name}"
        try:
            columns = pd.read_sql(query, self.db_engine)
            print(f"Columns in {table_name}: {columns['Field'].tolist()}")
            return columns['Field'].tolist()
        except Exception as e:
            print(f"Error fetching columns from table {table_name}: {e}")
            return []

    def fetch_data_from_db(self, table_name, start_date=None, end_date=None):
        columns = self.fetch_table_columns(table_name)

        # Adjust the columns based on your database schema
        date_col = 'date_time' if 'date_time' in columns else 'Date'
        query = f"SELECT {date_col} as Date, open as Open, high as High, low as Low, close as Close, volume as Volume FROM {table_name}"

        if start_date and end_date:
            query += f" WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'"

        try:
            df = pd.read_sql(query, self.db_engine)
            # print(f"Data fetched from {table_name}:")
            # print(df.head())
            return df
        except Exception as e:
            print(f"Error fetching data from MySQL table {table_name}: {e}")
            return pd.DataFrame()

    def calculate_indicators(self, df):

        if len(df) < 200:
            print("Not enough data to calculate indicators")
            return df

        # EMA 9, 20, 200
        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()

        # VWAP
        df['Session'] = (df['Date'].dt.date != df['Date'].shift(1).dt.date).cumsum()
        df['Typical_Price'] = (df['Close'] + df['High'] + df['Low']) / 3
        df['Cumulative_Typical_Price_Volume'] = (df['Typical_Price'] * df['Volume']).groupby(df['Session']).cumsum()
        df['Cumulative_Volume'] = df['Volume'].groupby(df['Session']).cumsum()
        df['VWAP'] = df['Cumulative_Typical_Price_Volume'] / df['Cumulative_Volume']

        # Bollinger Bands
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['BB_Middle'] = df['SMA20']
        df['BB_Upper'] = df['SMA20'] + 2 * df['Close'].rolling(window=20).std()
        df['BB_Lower'] = df['SMA20'] - 2 * df['Close'].rolling(window=20).std()

        # MACD
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

        # Remove intermediate calculation columns safely
        columns_to_remove = ['SMA20', 'Typical_Price', 'Cumulative_Typical_Price_Volume', 'Cumulative_Volume']
        df = df.drop(columns=[col for col in columns_to_remove if col in df])

        return df
    # 8 Columns for entry and exit for each position where if one of the criteria is true then it says true and
    # The true criteria are noted -WORKS

    # def generate_signals(self, df):
    #     # Initialize the signals columns
    #     df['Long_Entry'] = False
    #     df['Long_Entry_Criteria'] = ""
    #     df['Short_Entry'] = False
    #     df['Short_Entry_Criteria'] = ""
    #     df['Long_Exit'] = False
    #     df['Long_Exit_Criteria'] = ""
    #     df['Short_Exit'] = False
    #     df['Short_Exit_Criteria'] = ""
    #
    #     in_long_position = False
    #     in_short_position = False
    #
    #     for i in range(1, len(df)):
    #         if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(df['EMA20'].iloc[i]) or pd.isna(
    #                 df['EMA200'].iloc[i]):
    #             continue
    #
    #         long_entry_conditions = []
    #         short_entry_conditions = []
    #
    #         # Check Long Entry criteria
    #         if df['EMA9'].iloc[i] > df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] <= df['EMA20'].iloc[i - 1]:
    #             long_entry_conditions.append("EMA9 crossed above EMA20")
    #         if df['EMA9'].iloc[i] > df['EMA200'].iloc[i]:
    #             long_entry_conditions.append("EMA9 above EMA200")
    #         if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
    #             long_entry_conditions.append("EMA20 above EMA200")
    #         if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
    #             long_entry_conditions.append("Close above VWAP")
    #         if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] <= df['MACD_Signal'].iloc[
    #             i - 1]:
    #             long_entry_conditions.append("MACD crossed above Signal")
    #         if df['MACD'].iloc[i] > 0 and df['MACD'].iloc[i - 1] <= 0:
    #             long_entry_conditions.append("MACD crossed above 0")
    #         if df['Close'].iloc[i] < df['BB_Lower'].iloc[i] and df['Close'].iloc[i - 1] >= df['BB_Lower'].iloc[i - 1]:
    #             long_entry_conditions.append("Price crossed above BB Lower")
    #
    #         if long_entry_conditions:
    #             df.at[i, 'Long_Entry'] = True
    #             df.at[i, 'Long_Entry_Criteria'] = ', '.join(long_entry_conditions)
    #             in_long_position = True
    #
    #         # Check Short Entry criteria
    #         if df['EMA9'].iloc[i] < df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] >= df['EMA20'].iloc[i - 1]:
    #             short_entry_conditions.append("EMA9 crossed below EMA20")
    #         if df['EMA9'].iloc[i] < df['EMA200'].iloc[i]:
    #             short_entry_conditions.append("EMA9 below EMA200")
    #         if df['EMA20'].iloc[i] < df['EMA200'].iloc[i]:
    #             short_entry_conditions.append("EMA20 below EMA200")
    #         if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
    #             short_entry_conditions.append("Close below VWAP")
    #         if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] >= df['MACD_Signal'].iloc[
    #             i - 1]:
    #             short_entry_conditions.append("MACD crossed below Signal")
    #         if df['MACD'].iloc[i] < 0 and df['MACD'].iloc[i - 1] >= 0:
    #             short_entry_conditions.append("MACD crossed below 0")
    #         if df['Close'].iloc[i] > df['BB_Upper'].iloc[i] and df['Close'].iloc[i - 1] <= df['BB_Upper'].iloc[i - 1]:
    #             short_entry_conditions.append("Price crossed below BB Upper")
    #
    #         if short_entry_conditions:
    #             df.at[i, 'Short_Entry'] = True
    #             df.at[i, 'Short_Entry_Criteria'] = ', '.join(short_entry_conditions)
    #             in_short_position = True
    #
    #         # Check Long Exit criteria if in long position
    #         if in_long_position:
    #             long_exit_conditions = []
    #             if df['EMA9'].iloc[i] < df['EMA20'].iloc[i]:
    #                 long_exit_conditions.append("EMA9 below EMA20")
    #             if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
    #                 long_exit_conditions.append("Close below VWAP")
    #             if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i]:
    #                 long_exit_conditions.append("MACD below Signal")
    #             if df['Close'].iloc[i] >= df['BB_Upper'].iloc[i]:
    #                 long_exit_conditions.append("Price touches BB Upper")
    #
    #             if long_exit_conditions:
    #                 df.at[i, 'Long_Exit'] = True
    #                 df.at[i, 'Long_Exit_Criteria'] = ', '.join(long_exit_conditions)
    #                 in_long_position = False
    #
    #         # Check Short Exit criteria if in short position
    #         if in_short_position:
    #             short_exit_conditions = []
    #             if df['EMA9'].iloc[i] > df['EMA20'].iloc[i]:
    #                 short_exit_conditions.append("EMA9 above EMA20")
    #             if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
    #                 short_exit_conditions.append("Close above VWAP")
    #             if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i]:
    #                 short_exit_conditions.append("MACD above Signal")
    #             if df['Close'].iloc[i] <= df['BB_Lower'].iloc[i]:
    #                 short_exit_conditions.append("Price touches BB Lower")
    #
    #             if short_exit_conditions:
    #                 df.at[i, 'Short_Exit'] = True
    #                 df.at[i, 'Short_Exit_Criteria'] = ', '.join(short_exit_conditions)
    #                 in_short_position = False
    #
    #     return df

    def generate_signals(self, df):

        required_columns = ['EMA9', 'EMA20', 'EMA200', 'VWAP', 'MACD', 'MACD_Signal', 'BB_Upper', 'BB_Lower']
        for col in required_columns:
            if col not in df.columns:
                print(f"Not enough data to calculate {col}")
                return df

        # Initialize the signals columns
        criteria_columns = [
            'EMA9_above_EMA20', 'EMA9_below_EMA20', 'EMA9_above_EMA200', 'EMA9_below_EMA200',
            'EMA20_above_EMA200', 'EMA20_below_EMA200', 'Close_above_VWAP', 'Close_below_VWAP',
            'MACD_above_Signal', 'MACD_below_Signal', 'MACD_above_zero', 'MACD_below_zero',
            'Price_crossed_above_BB_Lower', 'Price_crossed_below_BB_Upper', 'Price_touches_BB_Upper',
            'Price_touches_BB_Lower'
        ]

        for col in criteria_columns:
            df[col] = ""

        df['Long_Entry'] = False
        df['Short_Entry'] = False
        df['Long_Exit'] = False
        df['Short_Exit'] = False

        in_long_position = False
        in_short_position = False

        for i in range(1, len(df)):
            if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(df['EMA20'].iloc[i]) or pd.isna(
                    df['EMA200'].iloc[i]):
                continue

            # Long Entry Criteria
            if df['EMA9'].iloc[i] > df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] <= df['EMA20'].iloc[i - 1]:
                df.at[i, 'EMA9_above_EMA20'] += "Long Entry, "
            if df['EMA9'].iloc[i] > df['EMA200'].iloc[i]:
                df.at[i, 'EMA9_above_EMA200'] += "Long Entry, "
            if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
                df.at[i, 'EMA20_above_EMA200'] += "Long Entry, "
            if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                df.at[i, 'Close_above_VWAP'] += "Long Entry, "
            if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] <= df['MACD_Signal'].iloc[
                i - 1]:
                df.at[i, 'MACD_above_Signal'] += "Long Entry, "
            if df['MACD'].iloc[i] > 0 and df['MACD'].iloc[i - 1] <= 0:
                df.at[i, 'MACD_above_zero'] += "Long Entry, "
            if df['Close'].iloc[i] < df['BB_Lower'].iloc[i] and df['Close'].iloc[i - 1] >= df['BB_Lower'].iloc[i - 1]:
                df.at[i, 'Price_crossed_above_BB_Lower'] += "Long Entry, "

            # Short Entry Criteria
            if df['EMA9'].iloc[i] < df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] >= df['EMA20'].iloc[i - 1]:
                df.at[i, 'EMA9_below_EMA20'] += "Short Entry, "
            if df['EMA9'].iloc[i] < df['EMA200'].iloc[i]:
                df.at[i, 'EMA9_below_EMA200'] += "Short Entry, "
            if df['EMA20'].iloc[i] < df['EMA200'].iloc[i]:
                df.at[i, 'EMA20_below_EMA200'] += "Short Entry, "
            if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                df.at[i, 'Close_below_VWAP'] += "Short Entry, "
            if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] >= df['MACD_Signal'].iloc[
                i - 1]:
                df.at[i, 'MACD_below_Signal'] += "Short Entry, "
            if df['MACD'].iloc[i] < 0 and df['MACD'].iloc[i - 1] >= 0:
                df.at[i, 'MACD_below_zero'] += "Short Entry, "
            if df['Close'].iloc[i] > df['BB_Upper'].iloc[i] and df['Close'].iloc[i - 1] <= df['BB_Upper'].iloc[i - 1]:
                df.at[i, 'Price_crossed_below_BB_Upper'] += "Short Entry, "

            # Long Exit Criteria
            if in_long_position:
                if df['EMA9'].iloc[i] < df['EMA20'].iloc[i]:
                    df.at[i, 'EMA9_below_EMA20'] += "Long Exit, "
                if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                    df.at[i, 'Close_below_VWAP'] += "Long Exit, "
                if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i]:
                    df.at[i, 'MACD_below_Signal'] += "Long Exit, "
                if df['Close'].iloc[i] >= df['BB_Upper'].iloc[i]:
                    df.at[i, 'Price_touches_BB_Upper'] += "Long Exit, "

            # Short Exit Criteria
            if in_short_position:
                if df['EMA9'].iloc[i] > df['EMA20'].iloc[i]:
                    df.at[i, 'EMA9_above_EMA20'] += "Short Exit, "
                if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                    df.at[i, 'Close_above_VWAP'] += "Short Exit, "
                if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i]:
                    df.at[i, 'MACD_above_Signal'] += "Short Exit, "
                if df['Close'].iloc[i] <= df['BB_Lower'].iloc[i]:
                    df.at[i, 'Price_touches_BB_Lower'] += "Short Exit, "

            # Update position flags
            df.at[i, 'Long_Entry'] = "Long Entry" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', '))
            df.at[i, 'Short_Entry'] = "Short Entry" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', '))
            df.at[i, 'Long_Exit'] = "Long Exit" in df.iloc[i][criteria_columns].apply(
                lambda x: x.strip(', ')) and in_long_position
            df.at[i, 'Short_Exit'] = "Short Exit" in df.iloc[i][criteria_columns].apply(
                lambda x: x.strip(', ')) and in_short_position

            if df.at[i, 'Long_Entry']:
                in_long_position = True
            if df.at[i, 'Short_Entry']:
                in_short_position = True
            if df.at[i, 'Long_Exit']:
                in_long_position = False
            if df.at[i, 'Short_Exit']:
                in_short_position = False

        # Remove trailing commas and spaces from criteria columns
        for col in criteria_columns:
            df[col] = df[col].str.strip(', ')

        # Drop the entry/exit columns
        df = df.drop(columns=['Long_Entry', 'Short_Entry', 'Long_Exit', 'Short_Exit'])

        return df

    def resample_data(self, df, interval):
        resampled_df = df.resample(interval, on='Date').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna().reset_index()

        return resampled_df

    def update_plot(self, days=4, interval='1min'):
        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date)
        combined_data = pd.concat([
            df_minute,
            pd.DataFrame(self.real_time_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        ])

        combined_data['Date'] = pd.to_datetime(combined_data['Date'])
        combined_data.sort_values(by='Date', inplace=True)
        combined_data = self.calculate_indicators(combined_data)

        resampled_data = self.resample_data(combined_data, interval)
        resampled_data = self.calculate_indicators(resampled_data)
        resampled_data = self.generate_signals(resampled_data)

        # print("Combined Data:")
        # print(combined_data)
        #
        # return combined_data

        print("Combined Data resampled:")
        print(resampled_data.tail())

        self.export_to_excel(resampled_data)

        return resampled_data

    def export_to_excel(self, df, filename="output.xlsx"):
        df.to_excel(filename, index=False)

    def convert_to_ny_time(self, df, date_col):
        greek_tz = pytz.timezone('Europe/Athens')
        ny_tz = pytz.timezone('America/New_York')

        df[date_col] = pd.to_datetime(df[date_col])
        df[date_col] = df[date_col].dt.tz_localize(greek_tz).dt.tz_convert(ny_tz).dt.tz_localize(None)

        return df

    def update_database_time_to_ny(self):
        df = self.load_data_from_db('minute_data')
        df = self.convert_to_ny_time(df, 'date_time')
        self.update_data_in_db(df, 'minute_data', 'temp_minute_data')

    def load_data_from_db(self, table_name):
        print(f"Attempting to load data from table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        print(f"SQL Query: {query}")

        try:
            df = pd.read_sql(query, self.db_engine)
            print("Data loaded successfully")
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return pd.DataFrame()

    def update_data_in_db(self, df, table_name, temp_table_name):
        # Save data to a temporary table
        df.to_sql(temp_table_name, self.db_engine, if_exists='replace', index=False)
        print("Data saved to temporary table successfully")

        # Update the original table from the temporary table
        with self.db_engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {table_name} t
                JOIN {temp_table_name} temp ON t.id = temp.id
                SET t.date_time = temp.date_time
            """))
        print("Data updated successfully")

    def clear_data_from_table(self, table_name):
        try:
            self.ensure_connection()
            query = text(f"TRUNCATE TABLE {table_name}")
            self.db_session.execute(query)
            self.db_session.commit()
            print(f"All data from {table_name} has been deleted.")
        except SQLAlchemyError as e:
            print(f"Error deleting data from {table_name}: {e}")
            self.db_session.rollback()

    def validate_interval(self, user_input):
        valid_intervals = ['1min', '2min', '3min', '4min', '5min', '10min', '15min', '30min', '1H']
        if user_input in valid_intervals:
            return user_input
        else:
            print(f"Invalid interval '{user_input}', defaulting to '5min'")
            return '5min'

    def main_thread_function(self, interval):
        while True:
            if not self.data_ready_queue.empty():
                _ = self.data_ready_queue.get()
                self.update_plot(interval=interval)

    def close_connection(self):
        if self.db_session:
            self.db_session.close()
            self.db_session = None  # Ensure the session is set to None after closing

app = TradingApp()
app.connect("127.0.0.1",7497, 1)

t1 = threading.Thread(target=app.run)
t1.start()

interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")

interval = app.validate_interval(interval_input)  # Χρήση της validate_interval για έλεγχο της εισόδου


# valid_intervals = ['1T', '1min', '5T', '5min', '10T', '15T', '30T', '1H']
# if interval not in valid_intervals:
#     print(f"Invalid interval: {interval}. Defaulting to '5T'.")
#     interval = '5T'

main_thread = threading.Thread(target=app.main_thread_function, args=(interval,))
main_thread.start()

while app.nextValidOrderId is None:
    print("Waiting for TWS connection aknowledgment...")
    time.sleep(1)

print("connection established")

# Converts data in db to New York Time
# app.update_database_time_to_ny()

# Clear existing data in the tables
# app.clear_data_from_table('minute_data')
# app.clear_data_from_table('daily_data')

# Historical data
contract = Contract()
contract.symbol = "AAPL"
contract.secType = "STK"
contract.exchange = "SMART"
contract.primaryExchange = "NASDAQ"
contract.currency = "USD"
print("Contract OK")

# Request historical minute data
# print("Requesting minute data")
# app.reqHistoricalData(
#     1,  # reqId for minute data
#     contract,  # contract details
#     "",  # end date/time (empty string for current date/time
#     "2 D",  # duration (2 months)
#     "1 min",  # bar size (1 minute)
#     "TRADES",  # data type
#     0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
#     1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
#     False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
#     []
# )
#
# # Request historical daily data
# print("Requesting daily data")
# app.reqHistoricalData(
#     2,  # reqId for daily data
#     contract,  # contract details
#     "",  # end date/time (empty string for current date/time
#     "2 D",  # duration (1 year)
#     "1 day",  # bar size (1 day)
#     "TRADES",  # data type
#     0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
#     1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
#     False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
#     []
# )

# Request market data for real-time updates
app.reqMktData(
    3,  # reqId
    contract,  # contract details
    "",  # generic tick l ist
    False,  # snapshot
    False,  # regulatory snapshot
    []  # options
)

# Dash application
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# dash_app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
#
# dash_app.layout = html.Div(children=[
#     html.H1(children='AAPL Candlestick Chart'),
#     dcc.Graph(id='live-update-graph'),
#     dcc.Interval(
#         id='interval-component',
#         interval=1*1000,  # in milliseconds
#         n_intervals=0
#     )
# ])
#
# @dash_app.callback(Output('live-update-graph', 'figure'),
#               [Input('interval-component', 'n_intervals')])
# def update_graph_live(n):
#     df = app.update_plot()
#
#     if df.empty:
#         print("No data available to plot.")
#         return go.Figure()
#
#     print("Data passed to the graph:")
#     print(df.tail())  # Print the last few rows of the data passed to the graph
#
#     fig = go.Figure(data=[go.Candlestick(
#         x=df['Date'],
#         open=df['Open'],
#         high=df['High'],
#         low=df['Low'],
#         close=df['Close']
#     )])
#
#     fig.update_layout(
#         title='AAPL Candlestick Chart',
#         xaxis_title='Date',
#         yaxis_title='Price'
#     )
#
#     return fig
#
# if __name__ == '__main__':
#     dash_app.run_server(debug=True, use_reloader=False)

app.close_connection()
