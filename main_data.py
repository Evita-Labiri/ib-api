import threading
import time
from ib_api import IBApi
from data_processing import DataProcessor
from database import Database

def main():
    db = Database()
    app = IBApi(data_processor=None, db=db)
    data_processor = DataProcessor(db, app)
    app.data_processor = data_processor
    app.connect("127.0.0.1", 7497, 1)

    t1 = threading.Thread(target=app.run)
    t1.start()

    # Default contract for testing
    default_symbol = "AAPL"
    default_secType = "STK"
    default_exchange = "SMART"
    default_currency = "USD"

    # Contract
    symbol = input(f"Enter the symbol (e.g., '{default_symbol}'): ").upper() or default_symbol
    secType = input(f"Enter the security type (e.g., '{default_secType}'): ").upper() or default_secType
    exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
    currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency

    contract = app.create_contract(symbol, secType, exchange, currency)
    app.ticker = symbol

    # Choose interval to show on the df
    interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
    interval = data_processor.validate_interval(interval_input)
    data_processor.interval = interval

    while app.nextValidOrderId is None:
        print("Waiting for TWS connection acknowledgment...")
        time.sleep(1)

    print("Connection established, nextValidOrderId:", app.nextValidOrderId)

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

    # Request historical daily data
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
        3,
        contract,
        "",
        False,
        False,
        []
    )

    # Start a thread to process data and update the plot
    main_thread = threading.Thread(target=app.main_thread_function, args=(interval,))
    main_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted by user, closing connection...")
    finally:
        app.close_connection()


if __name__ == "__main__":
    main()
