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
    order_manager = data_processor.order_manager
    app.connect("127.0.0.1", 7497, 1)

    t1 = threading.Thread(target=app.run)
    t1.start()

    # Default contract for testing
    default_symbol = "AAPL"
    default_secType = "STK"
    default_exchange = "SMART"
    default_currency = "USD"

    outside_rth_input = input("Allow orders outside regular trading hours? (yes/no, default: no): ").lower() or "no"
    outside_rth = outside_rth_input == "yes"

    # Contract
    symbol = input(f"Enter the symbol (e.g., '{default_symbol}'): ").upper() or default_symbol
    secType = input(f"Enter the security type (e.g., '{default_secType}'): ").upper() or default_secType
    exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
    currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency

    # action = input("Enter action (BUY/SELL): ").upper()
    # quantity = int(input("Enter quantity: "))
    # profit_target = float(input("Enter profit target: "))
    # limit_price = float(input("Enter limit price: "))
    # stop_loss = float(input("Enter stop loss: "))

    while app.nextValidOrderId is None:
        print("Waiting for TWS connection acknowledgment...")
        time.sleep(1)

    print("Connection established, nextValidOrderId:", app.nextValidOrderId)

    # Start the user command thread
    command_thread = threading.Thread(target=app.user_command_thread)
    command_thread.start()

    contract = app.create_contract(symbol, secType, exchange, currency)
    # app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss, outside_rth=outside_rth)

    # Request market data for real-time updates
    app.reqMktData(
        3,
        contract,
        "",
        False,
        False,
        []
    )

    # interval='1min'

    # Main script adjustments for main_order.py
    interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
    interval = data_processor.validate_interval(interval_input)
    data_processor.interval = interval

    # Start a thread to process data and update the plot
    main_thread = threading.Thread(target=app.order_main_thread_function, args=(data_processor, interval, contract, order_manager))
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
