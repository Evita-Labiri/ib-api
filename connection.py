import queue
from datetime import datetime, timedelta
from os import getenv

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
import dash
from dash import dcc, html
import plotly.graph_objects as go
from mysql.connector import errorcode, Error
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output
import mysql.connector

class TradingApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = []
        self.data_ready_queue = queue.Queue()
        self.lock = threading.Lock()
        self.historical_data_downloaded = False  # Flag to check if historical data is downloaded

        self.db_conn = self.create_connection()
        self.cursor = self.db_conn.cursor() if self.db_conn else None

    def create_connection(self):
        try:
            connect_timeout = 28800  # 8 ώρες
            connection = mysql.connector.connect(
                host="localhost",
                user=getenv("STOCKDATADB_UN"),
                password=getenv("STOCKDATADB_PASS"),
                database="stockdatadb"
            )
            if connection.is_connected():
                print("Successfully connected to the database")
            return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None

    def ensure_connection(self):
        if self.db_conn and not self.db_conn.is_connected():
            print("Reconnecting to the database...")
            self.db_conn = self.create_connection()
            self.cursor = self.db_conn.cursor() if self.db_conn else None

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}')
        if tickType == 4:  # Last price
            with self.lock:
                self.real_time_data.append(
                    [pd.Timestamp.now(), price, price, price, price, 0])  # Placeholder for OHLCV data
                self.data_ready_queue.put(self.real_time_data[-1])

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

    def main_thread_function(self):
        while True:
            if not self.data_ready_queue.empty():
                data = self.data_ready_queue.get()
                self.update_plot()

    def tickSize(self, reqId, tickType, size):
        print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}')

    def update_plot(self):
        combined_data = self.data + self.real_time_data
        df = pd.DataFrame(combined_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        return df

    # Εισαγωγή δεδομένων στη βάση δεδομένων
    def insert_data_to_db(self, df, table_name):
        for index, row in df.iterrows():
            self.ensure_connection()
            self.cursor.execute(f"""
                INSERT INTO {table_name} (ticker, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['ticker'], row['datetime'], row['open'], row['high'], row['low'], row['close'], row['volume']))
        self.db_conn.commit()

    def insert_data_to_minute_table(self, table_name, date, open, high, low, close, volume):
        ticker = "AAPL"  # Example ticker, you can change it as needed
        try:
            self.ensure_connection()
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = %s AND date_time = %s",
                                (ticker, date))

            row = self.cursor.fetchone()

            # if self.cursor.fetchone()[0] == 0:  # No record found
            if row and row[0] == 0:
                print(f"Inserting minute data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                self.cursor.execute(f"""
                       INSERT INTO {table_name} (ticker, date_time, open, high, low, close, volume)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """, (ticker, date, open, high, low, close, volume))
                self.db_conn.commit()
                print("Data inserted")

            else:
                print(f"Duplicate minute data found for {ticker} at {date}, skipping insertion.")
        except Error as e:
            print(f"Error inserting minute data into MySQL table: {e}")


    def insert_data_to_daily_table(self, table_name, date, open, high, low, close, volume):
        ticker = "AAPL"  # Example ticker, you can change it as needed
        try:
            self.ensure_connection()
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = %s AND date = %s", (ticker, date))
            #Check data
            row = self.cursor.fetchone()
            if row and row [0] == 0:
                print(f"Inserting daily data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                self.cursor.execute(f"""
                       INSERT INTO {table_name} (ticker, date, open, high, low, close, volume)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """, (ticker, date, open, high, low, close, volume))
                self.db_conn.commit()
                print("Data inserted")
            else:
                print(f"Duplicate daily data found for {ticker} at {date}, skipping insertion.")
        except Error as e:
            print(f"Error inserting daily data into MySQL table: {e}")

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()

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
print("Requesting minute data")
app.reqHistoricalData(
    1,  # reqId for minute data
    contract,  # contract details
    "",  # end date/time (empty string for current date/time
    "2 M",  # duration (1 days)
    "1 min",  # bar size (1 minute)
    "TRADES",  # data type
    0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
    1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
    False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
    []
)

# Request historical daily data
print("Requesting daily data")
app.reqHistoricalData(
    2,  # reqId for daily data
    contract,  # contract details
    "",  # end date/time (empty string for current date/time
    "1 Y",  # duration (1 year)
    "1 day",  # bar size (1 day)
    "TRADES",  # data type
    0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
    1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
    False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
    []
)

app.close_connection()
