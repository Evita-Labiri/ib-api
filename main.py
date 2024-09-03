import threading
import time

from globals import decision_queue, stop_flag
from ib_api import IBApi
from data_processing import DataProcessor
from database import Database
from order_manager import OrderManager


def run_data_script():
    db = Database()
    app = IBApi(data_processor=None, db=db)
    data_processor = DataProcessor(db, app)
    app.data_processor = data_processor
    app.connect("127.0.0.1", 7497, 1)

    t1 = threading.Thread(target=app.run)
    t1.start()

    default_ticker = "AAPL"
    default_sec_type = "STK"
    default_exchange = "SMART"
    default_currency = "USD"
    default_data_type = "minute"

    ticker = input(f"Enter the ticker (e.g., '{default_ticker}'): ").upper() or default_ticker
    sec_type = input(f"Enter the security type (e.g., '{default_sec_type}'): ").upper() or default_sec_type
    exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
    currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency
    data_type = input(f"Enter the data_type (e.g., '{default_data_type}'): ").upper() or default_data_type


    # reqId = 3
    contract = app.create_contract(ticker, sec_type, exchange, currency, data_type)
    # app.contracts.append({'reqId': reqId, 'contract': contract})
    app.set_ticker(ticker)

    interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
    interval = data_processor.validate_interval(interval_input)
    data_processor.interval = interval

    while app.nextValidOrderId is None:
        print("Waiting for TWS connection acknowledgment...")
        time.sleep(1)

    print("Connection established, nextValidOrderId:", app.nextValidOrderId)

    # Request historical minute data
    print("Requesting minute data")
    app.update_minute_data_for_symbol(contract)
    # app.reqHistoricalData(
    #     1,  # reqId for minute data
    #     contract,  # contract details
    #     "",  # end date/time (empty string for current date/time
    #     "4 D",  # duration (2 months)
    #     "1 min",  # bar size (1 minute)
    #     "TRADES",  # data type
    #     0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
    #     1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
    #     False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
    #     []
    # )

    # Request historical daily data
    # print("Requesting daily data")
    # app.reqHistoricalData(1

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
    #
    time.sleep(2)

    print(f"Requesting real-time data for {contract.symbol} on {contract.exchange}")
    req_id_for_rt = app.get_reqId_for_contract(contract)
    if req_id_for_rt is not None:
        app.reqMktData(
            req_id_for_rt,
            contract,
            "",
            False,  # False σημαίνει συνεχής ροή δεδομένων, όχι μόνο snapshot
            False,
            []
        )
    else:
        print("Failed to obtain request ID for the contract.")
    main_thread = threading.Thread(target=app.main_thread_function, args=(interval,))
    main_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted by user, closing connection...")
    finally:
        app.close_connection()


def run_order_script():
    stop_flag = threading.Event()
    decision_flag = threading.Event()
    db = Database()
    app = IBApi(data_processor=None, db=db)
    data_processor = DataProcessor(db, app)
    app.data_processor = data_processor
    order_manager = OrderManager()
    data_processor.order_manager = order_manager
    app.connect("127.0.0.1", 7497, 1)

    t1 = threading.Thread(target=app.run)
    t1.start()

    # contracts = []

    while True:
        outside_rth_input = input("Allow orders outside regular trading hours? (yes/no, default: no): ").lower() or "no"
        outside_rth = outside_rth_input == "yes"

        if not outside_rth and not order_manager.is_market_open():
            print("The market is closed. Please allow orders outside regular trading hours or try again during market hours.")
            retry_choice = input(
                "Do you want to try again or exit? (type 'retry' to try again, 'exit' to return to main menu): ").lower()
            if retry_choice == 'exit':
                return
            continue
        else:
            break

    while True:
        default_symbol = "AAPL"
        default_secType = "STK"
        default_exchange = "SMART"
        default_currency = "USD"
        default_data_type = "minute"

        symbol = input(f"Enter the symbol (e.g., '{default_symbol}'): ").upper() or default_symbol
        secType = input(f"Enter the security type (e.g., '{default_secType}'): ").upper() or default_secType
        exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
        currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency
        data_type = input(f"Enter the data_type (e.g., '{default_data_type}'): ").upper() or default_data_type

        while app.nextValidOrderId is None:
            print("Waiting for TWS connection acknowledgment...")
            time.sleep(1)

        print("Connection established, nextValidOrderId:", app.nextValidOrderId)

        # reqId = 3
        contract = app.create_contract(symbol, secType, exchange, currency, data_type)
        print(f"Created contract: {contract}")
        req_id = app.get_reqId_for_contract(contract)
        app.contracts.append({'reqId': req_id, 'contract': contract})
        print("Current list of contracts:")
        for idx, contract_entry in enumerate(app.contracts, start=1):
            print(f"{idx}: reqId={contract_entry['reqId']}, contract={contract_entry['contract']}")

        order_manager.initialize_contract(symbol)
        # Ενημέρωση δεδομένων πριν την επεξεργασία
        app.update_minute_data_for_symbol(contract)

        time.sleep(80)
        add_another = input("Do you want to add another contract? (yes/no): ").strip().lower()
        if add_another != 'yes':
            break

    print("Press ESC any time...")
    esc_listener_thread = threading.Thread(target=order_manager.listen_for_esc, args=(decision_queue, stop_flag))
    esc_listener_thread.start()

    # action = input("Enter action (BUY/SELL): ").upper()
    # quantity = int(input("Enter quantity: "))
    # limit_price = float(input("Enter limit price: "))
    # profit_target = float(input("Enter profit target: "))
    # stop_loss = float(input("Enter stop loss: "))
    # app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss)

    interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
    interval = data_processor.validate_interval(interval_input)
    data_processor.interval = interval

    time.sleep(2)
    req_id_for_rt = app.get_reqId_for_contract(contract)
    app.reqMktData(
        req_id_for_rt,
        contract,
        "",
        False,
        False,
        []
    )

    # # 7200
    # timer = threading.Timer(10, OrderManager.terminate_program)
    # timer.start()

    print("Following main thread: ")
    main_thread = threading.Thread(target=app.order_main_thread_function,
                                   args=(data_processor, interval, app.contracts, order_manager, decision_queue, decision_flag))
    main_thread.start()

    print("Decision thread start")
    decision_thread = threading.Thread(target=order_manager.handle_decision,
                                       args=(app, decision_queue, stop_flag))
    decision_thread.start()

    try:
        while not stop_flag.is_set():
            print("Running order management...")
            time.sleep(5)  # Μικρή καθυστέρηση για την αποφυγή υπερβολικής χρήσης CPU
            if stop_flag.is_set():
                print("Stop flag detected. Exiting main loop.")

    except KeyboardInterrupt:
        print("Interrupted by user, closing connection...")
    finally:
        app.close_connection()
        stop_flag.set()
        decision_flag.set()
        decision_thread.join()
        esc_listener_thread.join()
        main_thread.join()
        print("All threads have been terminated.")

    print("Order script stopped after 2 hours.")

#         gia close me kleisti agora prepei na nai limit order kai fill outside reg hours

def main():
    while True:
        print("Select the script to run:")
        print("1. Data Script")
        print("2. Order Script")
        print("3. Exit")
        choice = input("Enter 1, 2 or 3: ")

        if choice == '1':
            run_data_script()
            break
        elif choice == '2':
            run_order_script()
        elif choice == '3':
            print("Exiting.")
            run_order_script()
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
