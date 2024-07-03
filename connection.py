import os
import queue
from datetime import datetime, timedelta

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
            if reqId == 1:  # Minute data
                date_str, time_str, tz_str = bar.date.split()
                date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
            elif reqId == 2:  # Daily data
                date_str = bar.date
                date = datetime.strptime(date_str, '%Y%m%d')
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
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')
        if tickType == 4:  # Last
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
                self.update_plot()

    def tickSize(self, reqId, tickType, size):
        print("Tick Size called now: ")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

                self.update_plot()

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

    def update_plot(self, days=2):
        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date)
        # df_daily = self.fetch_data_from_db('daily_data')

        # Combine dataframes using pd.concat
        combined_data = pd.concat([
            df_minute,
            # df_daily,
            pd.DataFrame(self.real_time_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        ])

        combined_data['Date'] = pd.to_datetime(combined_data['Date'])
        combined_data.sort_values(by='Date', inplace=True)

        print("Combined Data:")
        # print(combined_data.tail())
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        # combined_data = pd.DataFrame(self.real_time_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        print(combined_data)
        return combined_data

    def main_thread_function(self):
        while True:
            if not self.data_ready_queue.empty():
                data = self.data_ready_queue.get()
                self.update_plot()

    def close_connection(self):
        if self.db_session:
            self.db_session.close()
            self.db_session = None  # Ensure the session is set to None after closing

app = TradingApp()
app.connect("127.0.0.1",7497, 1)

t1 = threading.Thread(target=app.run)
t1.start()

main_thread = threading.Thread(target=app.main_thread_function)
main_thread.start()

while app.nextValidOrderId is None:
    print("Waiting for TWS connection aknowledgment...")
    time.sleep(1)

print("connection established")

# Historical data
contract = Contract()
contract.symbol = "AAPL"
contract.secType = "STK"
contract.exchange = "SMART"
contract.primaryExchange = "NASDAQ"
contract.currency = "USD"
print("Contract OK")

#Request historical minute data
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

# Request historical daily data
# print("Requesting daily data")
# app.reqHistoricalData(
#     2,  # reqId for daily data
#     contract,  # contract details
#     "",  # end date/time (empty string for current date/time
#     "1 D",  # duration (1 year)
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
    "",  # generic tick list
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

# if __name__ == '__main__':
#     dash_app.run_server(debug=True, use_reloader=False)

app.close_connection()
