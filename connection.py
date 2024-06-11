import queue
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc

class TradingApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = []
        self.init_plot()
        self.data_ready_queue = queue.Queue()
        self.lock = threading.Lock()

    def init_plot(self):
        plt.ion()  # Interactive mode on
        self.fig, self.ax = plt.subplots()
        self.ax.set_title("AAPL Candlestick Chart")
        self.ax.set_xlabel('Date')
        self.ax.set_ylabel('Price')

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}')
        if tickType == 4:  # Last price
            self.real_time_data.append([time.time(), price, price, price, price, 0])  # Placeholder for OHLCV data
            self.update_chart(self.data)

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson = ""):
        print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId

    def historicalData(self, reqId, bar):
        print("Requesting data...")
        print(
            f'Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}')
        date_str = bar.date.split(' ')[0]  # Extract date string without timezone
        print("Date string:", date_str)
        date = datetime.strptime(date_str, '%Y%m%d')
        self.data.append([date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def historicalDataEnd(self, reqId, start, end):
        print("Historical data download complete")
        self.data_ready_queue.put(self.data)  # Put data in the queue for the main thread

    def main_thread_function(self):
        while True:
            if not self.data_ready_queue.empty():
                data = self.data_ready_queue.get()
                self.plot_candlestick(data)
    def tickPrice(self, reqId, tickType, price, attrib):
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}')
        if tickType == 4:  # Last price
            with self.lock:
                self.real_time_data.append([pd.Timestamp.now(), price, price, price, price, 0])  # Placeholder for OHLCV data
                self.data_ready_queue.put(self.real_time_data[-1])
        # self.update_chart()

    def tickSize(self, reqId, tickType, size):
        print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}')

        # candle stick bar

    def plot_candlestick(self, data):
        # Combine historical and real-time data
        combined_data = self.data + self.real_time_data
        df = pd.DataFrame(combined_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Convert the 'Date' column to datetime format and set it as the index
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        # Resample the data to daily frequency if needed
        df = df.resample('min').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})

        # Reset the index to use it for plotting
        df.reset_index(inplace=True)

        # Convert the date to matplotlib's date format
        df['Date'] = df['Date'].apply(mdates.date2num)

        # Plot the candlestick chart
        self.ax.clear()
        candlestick_ohlc(self.ax, df[['Date', 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='g',
                         colordown='r')

        # Format the date on the x-axis
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        self.ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

        # Rotate and align the tick labels so they look better
        self.fig.autofmt_xdate()

        # Show the plot
        plt.draw()

    def update_chart(self):
        self.plot_candlestick(self.data)

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

# Request historical data
print("Requesting data")
app.reqHistoricalData(
    1, #reqId
    contract,   #contract details
    "", #end date/time (empty string for current date/time
    "6 M", #duration (2 years) #worked with 2 D #Error -1 2108 Market data farm connection is inactive but should be available upon demand.usfarm.nj
    "1 min", #bar size (1 minute)
    "TRADES",   #data type
    0,  #whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
    1,  #1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS).2 formats the date and time as Unix timestamps.
    False, #weather the client keep getting real-time updates of new data points or not(keep only the historical data after the initial receive).
    []
)

plt.show(block=True)













