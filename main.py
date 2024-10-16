import signal
import sys
import threading
import time
# from datetime import datetime,  time as dt_time
# import schedule

from globals import decision_queue
from ib_api import IBApi
from data_processing import DataProcessor
from database import Database
from order_manager import OrderManager
import logging


# def run_data_script():
#     db = Database()
#     app = IBApi(data_processor=None, db=db)
#     data_processor = DataProcessor(db, app)
#     app.data_processor = data_processor
#     app.connect("127.0.0.1", 7497, 1)
#
#     t1 = threading.Thread(target=app.run)
#     t1.start()
#
#     default_ticker = "AAPL"
#     default_sec_type = "STK"
#     default_exchange = "SMART"
#     default_currency = "USD"
#     default_data_type = "minute"
#
#     ticker = input(f"Enter the ticker (e.g., '{default_ticker}'): ").upper() or default_ticker
#     sec_type = input(f"Enter the security type (e.g., '{default_sec_type}'): ").upper() or default_sec_type
#     exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
#     currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency
#     data_type = input(f"Enter the data_type (e.g., '{default_data_type}'): ").upper() or default_data_type
#
#     # reqId = 3
#     contract = app.create_contract(ticker, sec_type, exchange, currency, data_type)
#     # app.contracts.append({'reqId': reqId, 'contract': contract})
#     app.set_ticker(ticker)
#
#     # interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
#     # interval = data_processor.validate_interval(interval_input)
#     # data_processor.interval = interval
#
#     interval_entry_input = input("Enter the resample interval for entry signals (e.g., '1min', '5min', '10min'): ")
#     interval_entry = data_processor.validate_interval(interval_entry_input)
#
#     interval_exit_input = input("Enter the resample interval for exit signals (e.g., '1min', '5min', '10min'): ")
#     interval_exit = data_processor.validate_interval(interval_exit_input)
#
#     data_processor.interval_entry = interval_entry  # Αποθηκεύουμε το interval για τα entry signals
#     data_processor.interval_exit = interval_exit  # Αποθηκεύουμε το interval για τα exit signals
#
#     while app.nextValidOrderId is None:
#         print("Waiting for TWS connection acknowledgment...")
#         time.sleep(1)
#
#     print("Connection established, nextValidOrderId:", app.nextValidOrderId)
#
#     # Request historical minute data
#     print("Requesting minute data")
#     app.update_minute_data_for_symbol(contract)
#     # app.reqHistoricalData(
#     #     1,  # reqId for minute data
#     #     contract,  # contract details
#     #     "",  # end date/time (empty string for current date/time
#     #     "4 D",  # duration (2 months)
#     #     "1 min",  # bar size (1 minute)
#     #     "TRADES",  # data type
#     #     0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
#     #     1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
#     #     False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
#     #     []
#     # )
#
#     # Request historical daily data
#     # print("Requesting daily data")
#     # app.reqHistoricalData(1
#
#     #     2,  # reqId for daily data
#     #     contract,  # contract details
#     #     "",  # end date/time (empty string for current date/time
#     #     "2 D",  # duration (1 year)
#     #     "1 day",  # bar size (1 day)
#     #     "TRADES",  # data type
#     #     0,  # whether to include only regular trading hours data (1) or to include all trading hours data (0) in the historical data request.
#     #     1,  # 1 formats the date and time as human-readable strings (YYYYMMDD HH:MM:SS). 2 formats the date and time as Unix timestamps.
#     #     False,  # whether the client keep getting real-time updates of new data points or not (keep only the historical data after the initial receive).
#     #     []
#     # )
#     #
#     time.sleep(2)
#
#     print(f"Requesting real-time data for {contract.symbol} on {contract.exchange}")
#     req_id_for_rt = app.get_reqId_for_contract(contract)
#     if req_id_for_rt is not None:
#         app.reqMktData(
#             req_id_for_rt,
#             contract,
#             "",
#             False,  # False σημαίνει συνεχής ροή δεδομένων, όχι μόνο snapshot
#             False,
#             []
#         )
#     else:
#         print("Failed to obtain request ID for the contract.")
#     # main_thread = threading.Thread(target=app.main_thread_function, args=(interval,))
#     main_thread = threading.Thread(target=app.main_thread_function, args=(interval_entry, interval_exit))
#     main_thread.start()
#
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("Interrupted by user, closing connection...")
#     finally:
#         app.close_connection()

def run_order_script():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler("ib_api.log")]
    )

    logger = logging.getLogger(__name__)
    # logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]
    logger.info("Starting order script...")
    print("Running order script")
    stop_flag = threading.Event()
    decision_flag = threading.Event()
    db = Database()
    app = IBApi(data_processor=None, db=db)
    data_processor = DataProcessor(db, app)
    app.data_processor = data_processor
    order_manager = OrderManager()
    data_processor.order_manager = order_manager
    logger.info("Connecting to TWS API...")
    app.connect("100.64.0.21", 7497, 1)
    # app.connect("100.64.0.69", 7497, 1)

    t1 = threading.Thread(target=app.run)
    t1.start()

    # User chooses to allow or not orders outside trading hours
    # while True:
        # outside_rth_input = input("Allow orders outside regular trading hours? (yes/no, default: no): ").lower() or "no"
        # outside_rth = outside_rth_input == "yes"
        #
        # logger.info(f"User chose to allow orders outside regular trading hours: {outside_rth}")

        # if not outside_rth and not order_manager.is_market_open():
        #     logger.info("The market is closed and the user chose not to allow orders outside trading hours.")
        #     print("The market is closed. Please allow orders outside regular trading hours or try again during market hours.")
        #
        #     retry_choice = input(
        #         "Do you want to try again or exit? (type 'retry' to try again, 'exit' to return to main menu): ").lower()
        #
        #     logger.info(f"User chose to {retry_choice} after market closed.")
        #
        #     if retry_choice == 'exit':
        #         logger.info("User chose to exit.")
        #         return
        #     continue
        # else:
        #     logger.info("Proceeding with order submission.")
        #     break
    logger.info("Fetching tickers from database...")
    print("Fetching tickers from db")
    tickers = db.get_tickers_from_db()

    if not tickers:
        logger.error("No tickers found for today's date.")
        return

    for ticker_entry in tickers:
        symbol = ticker_entry[0]
        print(f"Ticker entry: {ticker_entry}")

        date = ticker_entry[1]
        print(f"Processing symbol: {symbol} for date: {date}")

        logger.info(f"Processing symbol: {symbol} for date: {date}")

        default_symbol = "AAPL"
        default_secType = "STK"
        default_exchange = "SMART"
        default_currency = "USD"
        default_data_type = "minute"

        secType = default_secType
        exchange = default_exchange
        currency = default_currency
        data_type = default_data_type

        # symbol = input(f"Enter the symbol (e.g., '{default_symbol}'): ").upper() or default_symbol
        # secType = input(f"Enter the security type (e.g., '{default_secType}'): ").upper() or default_secType
        # exchange = input(f"Enter the exchange (e.g., '{default_exchange}'): ").upper() or default_exchange
        # currency = input(f"Enter the currency (e.g., '{default_currency}'): ").upper() or default_currency
        # data_type = input(f"Enter the data_type (e.g., '{default_data_type}'): ").upper() or default_data_type

        logger.info(
            f"User entered symbol: {symbol}, secType: {secType}, exchange: {exchange}, currency: {currency}, data_type: {data_type}")

        while app.nextValidOrderId is None:
            logger.debug("Waiting for TWS connection acknowledgment...")
            print("Waiting for TWS connection acknowledgment...")
            time.sleep(1)

        logger.info(f"Connection established, nextValidOrderId: {app.nextValidOrderId}")
        print("Connection established, nextValidOrderId:", app.nextValidOrderId)

        contract = app.create_contract(symbol, secType, exchange, currency, data_type)
        logger.info(f"Created contract for symbol: {symbol}, contract details: {contract}")
        print(f"Created contract: {contract}")
        req_id = app.get_reqId_for_contract(contract)
        app.contracts.append({'reqId': req_id, 'contract': contract})
        logger.info(f"Added contract to the list, current number of contracts: {len(app.contracts)}")
        # print("Current list of contracts:")

        # Logging statement
        for idx, contract_entry in enumerate(app.contracts, start=1):
            # print(f"{idx}: reqId={contract_entry['reqId']}, contract={contract_entry['contract']}")
            logger.debug(f"{idx}: reqId={contract_entry['reqId']}, contract={contract_entry['contract']}")

        order_manager.initialize_contract(symbol)
        app.data_download_complete = False
        logger.info(f"Starting data download for {symbol}")
        app.update_minute_data_for_symbol(contract)

        while not app.data_download_complete:
            # print("Waiting for data download to complete...")
            logger.debug("Waiting for data download to complete...")
            time.sleep(5)
        print(f"Data download for {symbol} is complete.")
    logger.info("All tickers processed.")
    print("All tickers processed.")

    # add_another = input("Do you want to add another contract? (yes/no): ").strip().lower()
    # logger.info(f"User chose to add another contract: {add_another}")
    # if add_another != 'yes':
    #     break

    logger.info("Pressing ESC will terminate the process.")
    print("Press ESC any time...")
    esc_listener_thread = threading.Thread(target=order_manager.listen_for_esc, args=(decision_queue, stop_flag))
    esc_listener_thread.start()

    # action = input("Enter action (BUY/SELL): ").upper()
    # quantity = int(input("Enter quantity: "))
    # limit_price = float(input("Enter limit price: "))
    # profit_target = float(input("Enter profit target: "))
    # stop_loss = float(input("Enter stop loss: "))
    # app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss)
    #
    # interval_input = input("Enter the resample interval (e.g., '1min', '5min', '10min'): ")
    # interval = data_processor.validate_interval(interval_input)
    # data_processor.interval = interval

    # interval_entry_input = input("Enter the resample interval for entry signals (e.g., '1min', '5min', '10min'): ")
    # interval_entry = data_processor.validate_interval(interval_entry_input)
    # logger.info(f"User entered interval for entry signals: {interval_entry_input}, validated as: {interval_entry}")
    #
    # interval_exit_input = input("Enter the resample interval for exit signals (e.g., '1min', '5min', '10min'): ")
    # interval_exit = data_processor.validate_interval(interval_exit_input)
    # logger.info(f"User entered interval for exit signals: {interval_exit_input}, validated as: {interval_exit}")

    interval_entry = '5min'
    interval_exit = '1min'

    data_processor.interval_entry = interval_entry
    data_processor.interval_exit = interval_exit

    # print(f"Type of contracts: {type(app.contracts)}")
    # print(f"Contracts list before loop: {app.contracts}")
    # print(f"Length of contracts: {len(app.contracts)}")

    # print("Requesting real time data")
    logger.info("Requesting real-time data for all contracts...")
    for contract_dict in app.contracts:
        contract = contract_dict['contract']
        logger.debug(f"Processing contract: {contract.symbol}")
        # print(f"Processing contract: {contract.symbol}")

        req_id_for_rt = app.get_reqId_for_contract(contract)
        # print(f"The req Id is: {req_id_for_rt}")
        logger.info(f"Request ID for {contract.symbol}: {req_id_for_rt}")

        if req_id_for_rt is not None:
            logger.info(f"Requesting real-time data for {contract.symbol} with req_id: {req_id_for_rt}")
            print(f"Requesting real-time data for {contract.symbol} with req_id: {req_id_for_rt}")
            app.reqMktData(
                req_id_for_rt,
                contract,
                "",
                False,
                False,
                []
            )
        else:
            logger.error(f"Failed to obtain request ID for the contract {contract.symbol}")
            print(f"Failed to obtain request ID for the contract {contract.symbol}")

    logger.info("Following main thread...")
    print("Following main thread: ")
    main_thread = threading.Thread(target=app.order_main_thread_function, args=(
        data_processor, interval_entry, interval_exit, app.contracts, order_manager, decision_queue, decision_flag))
    main_thread.start()

    logger.info("Decision thread starting...")
    print("Decision thread start")
    decision_thread = threading.Thread(target=order_manager.handle_decision,
                                       args=(app, decision_queue, decision_flag))
    decision_thread.start()

    logger.info("All tickers processed, starting export to Excel.")
    export_thread = threading.Thread(target= data_processor.export_to_excel_thread)
    export_thread.start()

    try:
        while not stop_flag.is_set():
            logger.debug(f"Running order management... (stop_flag: {stop_flag.is_set()})")
            print(f"Running order management... (stop_flag: {stop_flag.is_set()})")
            time.sleep(5)
            if stop_flag.is_set():
                logger.info("Stop flag detected. Exiting main loop.")
                print("Stop flag detected. Exiting main loop.")

            logger.debug(f"Decision thread status: {decision_thread.is_alive()}")
            logger.debug(f"Main thread status: {main_thread.is_alive()}")
            print(f"Decision thread status: {decision_thread.is_alive()}")
            print(f"Main thread status: {main_thread.is_alive()}")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user, closing connection...")
        # print("Interrupted by user, closing connection...")
    finally:
        logger.info("Finally clause activated. Closing connections and joining threads.")
        # print("Finally clause activated")
        if data_processor.export_buffer:
            data_processor.export_to_excel(data_processor.export_buffer, filename="final_output.xlsx")
        app.close_connection()
        stop_flag.set()
        decision_flag.set()
        decision_thread.join()
        esc_listener_thread.join()
        main_thread.join()
        logger.info("All threads have been terminated.")
        print("All threads have been terminated.")

# def main():
#     while True:
#         print("Select the script to run:")
#         print("1. Data Script")
#         print("2. Order Script")
#         print("3. Exit")
#         choice = input("Enter 1, 2 or 3: ")
#
#         if choice == '1':
#             logger.info("Running Data Script...")
#             # run_data_script()
#             break
#         elif choice == '2':
#             logger.info("Running Order Script...")
#             run_order_script()
#         elif choice == '3':
#             print("Exiting.")
#             logger.info("Exiting program...")
#             # Να φτιαξω exit στο main menu
#             # run_order_script()
#             break
#         else:
#             print("Invalid choice. Please try again.")

# if __name__ == "__main__":
#     current_time = datetime.now().time()
#     start_time = dt_time(23, 13)
#     stop_time = dt_time(11, 15)
#
#     if start_time <= current_time <= stop_time:
#         run_order_script()
#     else:
#         logger.info("Outside operational hours. The script will not run.")
run_order_script()

