import threading
from datetime import datetime, time, timedelta
from queue import Empty
from time import sleep
import pandas as pd
import pytz
import keyboard
import api_helper
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("ib_api.log")]
)
logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]


class OrderManager:
    def __init__(self):
        # self.app = app
        self.in_long_position = False
        self.in_short_position = False
        self.place_orders_outside_rth = False
        self.alert_active = {}
        self.positions = {}
        self.active_positions = []
        self.lock = threading.Lock()

    def set_order_outside_rth(self):
        while True:
            outside_rth_input = input(
                "Do you want to place orders outside regular trading hours? (yes/no): ").strip().lower()
            if outside_rth_input == 'yes':
                self.place_orders_outside_rth = True
                logger.info("Orders outside regular trading hours are allowed.")
                break
            elif outside_rth_input == 'no':
                logger.info("Orders outside regular trading hours are not allowed.")
                print("Orders outside regular trading hours are not allowed.")
                choice = input("Would you like to retry or return to the main menu? (retry/menu): ").strip().lower()
                if choice == 'retry':
                    continue  # Επιστρέφει στην αρχή της while loop για να προσπαθήσει ξανά
                elif choice == 'menu':
                    logger.info("Returning to the main menu.")
                    print("Returning to the main menu.")
                    return  # Επιστρέφει στο κεντρικό μενού (πρέπει να χειριστείς το επιστροφή στο μενού εκτός της συνάρτησης)
                else:
                    logger.warning("Invalid choice. Please try again.")
                    # print("Invalid choice. Please try again.")
            else:
                logger.warning("Invalid input. Please enter 'yes' or 'no'.")
                # print("Invalid input. Please enter 'yes' or 'no'.")

    def is_market_open(self):
        now = datetime.now(pytz.timezone('America/New_York'))
        logger.info(f"Current time in NY: {now}")
        # print(f"Current time in NY: {now}")
        market_open = time(9, 30)
        market_close = time(16, 0)
        if now.weekday() >= 5:  # If it's Saturday(5) or Sunday(6)
            logger.info("Market is closed because it's the weekend.")
            return False
        if market_open <= now.time() <= market_close:
            logger.info("Market is currently open.")
            print("Market is currently open.")
            return True
        else:
            logger.info("Market is closed at this time.")
            print("Market is closed at this time.")
            return False

    def get_order_details_from_user(self, action):
        quantity = int(input(f"Enter quantity for {action} order: "))
        limit_price = float(input(f"Enter limit price for {action} order: "))
        profit_target = float(input(f"Enter profit target for {action} order: "))
        stop_loss = float(input(f"Enter stop loss for {action} order: "))
        logger.info(f"Order details for {action}: quantity={quantity}, limit_price={limit_price}, profit_target={profit_target}, stop_loss={stop_loss}")
        return quantity, limit_price, profit_target, stop_loss

    def check_and_place_order(self, contract, action, quantity, limit_price, profit_target, stop_loss):
        import data_processing
        import database
        import ib_api
        db = database.Database
        data_processor = data_processing.DataProcessor(db)
        app = ib_api.IBApi(data_processor, db)

        if self.place_orders_outside_rth or self.is_market_open():
            app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss,
                                         outside_rth=self.place_orders_outside_rth)
            logger.info(f"Placed {action} order for {quantity} shares at {limit_price}")
            # print(f"Placed {action} order for {quantity} shares at {limit_price}")
        else:
            logger.warning("Market is closed and order placing outside RTH is not allowed.")
            # print("Market is closed and order placing outside RTH is not allowed.")

    def send_alert(self, message):
        logger.info(f"ALERT: {message}")
        # print(f"ALERT: {message}")

    # def wait_for_user_decision(self, message):
    #     while True:
    #         choice = input(message).lower()
    #         if choice in ['yes', 'no']:
    #             return choice
    #         print("Invalid input. Please enter 'yes' or 'no'.")
    #         sleep(1)

    def initialize_contract(self, contract_symbol):
        if contract_symbol not in self.positions:
            self.positions[contract_symbol] = {
                'in_long_position': False,
                'in_short_position': False,
                'order_id': None,
                'status': 'Closed'
            }
            self.alert_active[contract_symbol] = False
            logger.info(f"Initialized contract {contract_symbol} with default position values.")
            # print(f"Initialized contract {contract_symbol} with default position values.")
        else:
            logger.info(f"Contract {contract_symbol} already initialized.")
            # print(f"Contract {contract_symbol} already initialized.")

    # def add_position(self, position_type, order_id, status="Open"):
    #     position = {
    #         'type': position_type,
    #         'order_id': order_id,
    #         'status': status
    #     }
    #     self.positions.append(position)

    def update_position_status(self, order_id, new_status):
        for position in self.positions:
            if position['order_id'] == order_id:
                position['status'] = new_status
                # logger.info(f"Updated position {order_id} status to {new_status}")
                break

    def get_position_to_close(self, position_type):
        for position in reversed(self.positions):  # Ξεκινάμε από το τέλος της λίστας
            if position['type'] == position_type and position['status'] == 'Open':
                return position
        return None

    def open_long_position(self):
        self.in_long_position = True
        self.in_short_position = False
        # logger.info("Opened long position.")

    def open_short_position(self):
        self.in_short_position = True
        self.in_long_position = False
        # logger.info("Opened short position.")

    def close_long_position(self):
        self.in_long_position = False
        # logger.info("Closed long position.")

    def close_short_position(self):
        self.in_short_position = False
        # logger.info("Closed short position.")

    def process_signals_and_place_orders(self, df_entry, df_exit, decision_queue, decision_flag, contract_symbol):
        logger.info(f"Running process signals for {contract_symbol}")
        # print(f"Running process signals for {contract_symbol}")
        # with self.lock:
        signals = []

        if not isinstance(df_entry, pd.DataFrame):
            logger.error(f"Error: df_entry is not a DataFrame but a {type(df_entry)}")
            # print(f"Error: df_entry is not a DataFrame but a {type(df_entry)}")
            return
        if not isinstance(df_exit, pd.DataFrame):
            logger.error(f"Error: df_exit is not a DataFrame but a {type(df_exit)}")
            # print(f"Error: df_exit is not a DataFrame but a {type(df_exit)}")
            return

        if self.alert_active.get(contract_symbol, False):
            logger.info(f"Alert already active for {contract_symbol}, skipping.")
            # print(f"Alert already active for {contract_symbol}, skipping.")
            return

        try:
            df_entry['Date'] = pd.to_datetime(df_entry['Date'], errors='coerce').dt.tz_localize(None)
            df_exit['Date'] = pd.to_datetime(df_exit['Date'], errors='coerce').dt.tz_localize(None)

            i = len(df_entry) - 1
            timestamp = df_entry.iloc[i]['Date']

            if pd.api.types.is_datetime64_any_dtype(timestamp):
                timestamp = timestamp.tz_localize(None)

            # Παίρνουμε τα δεδομένα απευθείας από το DataFrame αντί για Series
            # long_entry = df_entry.at[i, 'Long_Entry'] if 'Long_Entry' in df_entry.columns else False
            # short_entry = df_entry.at[i, 'Short_Entry'] if 'Short_Entry' in df_entry.columns else False
            #
            # # Παίρνουμε τα δεδομένα από το df_exit για την ίδια χρονική στιγμή
            # if i < len(df_exit):
            #     long_exit = df_exit.at[i, 'Long_Exit'] if 'Long_Exit' in df_exit.columns else False
            #     short_exit = df_exit.at[i, 'Short_Exit'] if 'Short_Exit' in df_exit.columns else False
            # else:
            #     long_exit = False
            #     short_exit = False

            long_entry = df_entry.at[i, 'Long_Entry'] if 'Long_Entry' in df_entry.columns else False
            short_entry = df_entry.at[i, 'Short_Entry'] if 'Short_Entry' in df_entry.columns else False
            long_exit = df_exit.at[i, 'Long_Exit'] if 'Long_Exit' in df_exit.columns else False
            short_exit = df_exit.at[i, 'Short_Exit'] if 'Short_Exit' in df_exit.columns else False

            previous_close = df_entry.at[i - 1, 'Close'] if i > 0 else None

            # Έλεγχος για αντικρουόμενα σήματα entry και exit
            if (long_entry and long_exit) or (short_entry and short_exit):
                logger.warning(f"Conflicting signals for {contract_symbol}. No action taken.")
                print(f"Conflicting signals for {contract_symbol}. No action taken.")
                # continue
                self.alert_active[contract_symbol] = False
                return

            # Έλεγχος για ταυτόχρονα Long και Short Entry
            if long_entry and short_entry:
                logger.warning(f"Both Long Entry and Short Entry signals present for {contract_symbol}. No action taken.")
                print(
                    f"Both Long Entry and Short Entry signals present for {contract_symbol}. No action taken.")
                # continue
                self.alert_active[contract_symbol] = False
                return

            # Διαχείριση long entry signals
            if long_entry and not self.positions[contract_symbol]['in_long_position'] and not self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'long', i, previous_close, timestamp))
                logger.info(f"Long entry signal detected: {signals[-1]}")
                print(f"Long entry signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            # Διαχείριση short entry signals
            if short_entry and not self.positions[contract_symbol]['in_long_position'] and not self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'short', i, previous_close, timestamp))
                logger.info(f"Short entry signal detected: {signals[-1]}")
                print(f"Short entry signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            # Διαχείριση long exit signals
            if long_exit and self.positions[contract_symbol]['in_long_position']:
                signals.append((contract_symbol, 'exit_long', i, previous_close, timestamp))
                logger.info(f"Long exit signal detected: {signals[-1]}")
                print(f"Long exit signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            # Διαχείριση short exit signals
            if short_exit and self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'exit_short', i, previous_close, timestamp))
                logger.info(f"Short exit signal detected: {signals[-1]}")
                print(f"Short exit signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

        except KeyError as e:
            logger.error(f"KeyError at index {i}: {e}, skipping this index.")
            # print(f"KeyError at index {i}: {e}, skipping this index.")
            # continue
        except Exception as e:
            logger.error(f"Unexpected error at index {i}: {e}, skipping this index.")
            # print(f"Unexpected error at index {i}: {e}, skipping this index.")
            # continue

        # Αν υπάρχουν signals, τα στέλνουμε στην ουρά
        if signals:
            logger.info(f"Signals to be added to queue: {signals}")
            # print(f"Signals to be added to queue: {signals}")
            for signal in signals:
                decision_queue.put(signal)
                logger.info(f"Signal added to queue: {signal}")
                print(f"Signal added to queue: {signal}")
                # decision_flag.wait()
                decision_flag.set()
            decision_flag.clear()
            self.alert_active[contract_symbol] = False
        else:
            logger.info(f"No signals found for {contract_symbol}")
            print(f"No signals found for {contract_symbol}")

    def handle_decision(self, app, decision_queue, decision_flag):
        logger.info("Handle Decision running...")
        print("Handle Decision running...")
        while True:
            # if stop_flag.is_set():
            #     print("Stop flag detected in decision thread. Exiting.")
            #     break

            # with self.lock:
            try:
                logger.info("Waiting for signal from queue...")
                print("Waiting for signal from queue...")
                signal = decision_queue.get(timeout=30)
                logger.info(f"Signal received from queue: {signal}")
                print(f"Signal received from queue: {signal}")

                self.wait_for_market_time()

                contract_symbol, action, index, close_price, timestamp = signal
                logger.info(f"Handling decision: {signal}")
                print(f"Handling decision: {signal}")

                contract = next((c['contract'] for c in app.contracts if c['contract'].symbol == contract_symbol), None)
                if contract is None:
                    logger.error(f"Contract not found for symbol: {contract_symbol}")
                    print(f"Contract not found for symbol: {contract_symbol}")
                    continue

                while app.nextValidOrderId is None:
                    logger.info("Waiting for next valid order ID...")
                    print("Waiting for next valid order ID...")
                    sleep(1)
                logger.info(f"Next valid order ID is {app.nextValidOrderId}")
                print(f"Next valid order ID is {app.nextValidOrderId}")

                outside_rth = self.place_orders_outside_rth

                if action == 'long':
                    logger.info(f"Initializing contract for long position: {contract_symbol}")
                    # print(f"Initializing contract for long position: {contract_symbol}")
                    self.initialize_contract(contract_symbol)
                    logger.debug(f"Contract {contract_symbol} after initialization: {self.positions[contract_symbol]}")
                    # print(f"Contract {contract_symbol} after initialization: {self.positions[contract_symbol]}")

                    quantity = 100
                    limit_price = close_price - (30 / quantity)
                    profit_target_price = close_price + (100 / quantity)
                    stop_loss_price = close_price - (61 / quantity)

                    # orders = app.place_bracket_order(contract, "BUY", quantity, limit_price, profit_target_price,
                    #                                  stop_loss_price,
                    #                                  outside_rth=outside_rth)
                    # if orders:
                    #     print(f"Orders placed for {contract_symbol}: {orders}")
                    #     self.positions[contract_symbol]['in_long_position'] = True
                    #     self.positions[contract_symbol]['in_short_position'] = False
                    #     self.positions[contract_symbol]['order_id'] = app.nextValidOrderId
                    #     self.positions[contract_symbol]['status'] = 'Pre-Submitted'
                    #     print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                    #     self.display_positions()
                    # else:
                    #     print(f"Failed to place orders for {contract_symbol}.")

                    orders = app.place_bracket_order(contract, "BUY", quantity, limit_price, profit_target_price,
                                                     stop_loss_price,
                                                     outside_rth=outside_rth)
                    if orders:
                        logger.info(f"Orders placed for {contract_symbol}: {orders}")
                        print(f"Orders placed for {contract_symbol}: {orders}")
                        # Only update the position and order ID after a successful placement and increment the order ID
                        self.positions[contract_symbol]['in_long_position'] = True
                        self.positions[contract_symbol]['in_short_position'] = False
                        self.positions[contract_symbol]['order_id'] = orders[
                            0].orderId  # Ensure the correct order ID is assigned
                        sleep(2)
                        # self.positions[contract_symbol]['status'] = 'PreSubmitted'
                        logger.debug(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        # print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        self.display_positions()
                    else:
                        logger.error(f"Failed to place orders for {contract_symbol}.")
                        print(f"Failed to place orders for {contract_symbol}.")
                        self.alert_active[contract_symbol] = False

                elif action == 'short':
                    logger.info(f"Initializing contract for short position: {contract_symbol}")
                    # print(f"Initializing contract for short position: {contract_symbol}")
                    self.initialize_contract(contract_symbol)
                    logger.debug(f"Contract {contract_symbol} after initialization: {self.positions[contract_symbol]}")
                    # print(f"Contract {contract_symbol} after initialization: {self.positions[contract_symbol]}")

                    quantity = 100
                    limit_price = close_price + (30 / quantity)
                    profit_target_price = limit_price - (100 / quantity)
                    stop_loss_price = close_price + (61 / quantity)

                    orders = app.place_bracket_order(contract, "SELL", quantity, limit_price, profit_target_price,
                                                     stop_loss_price, outside_rth=outside_rth)
                    if orders:
                        logger.info(f"Orders placed for {contract_symbol}: {orders}")
                        # print(f"Orders placed for {contract_symbol}: {orders}")
                        self.positions[contract_symbol]['in_short_position'] = True
                        self.positions[contract_symbol]['in_long_position'] = False
                        self.positions[contract_symbol]['order_id'] = orders[
                            0].orderId  # Ensuring the correct order ID is assigned from the first order
                        sleep(2)
                        # self.positions[contract_symbol]['status'] = 'PreSubmitted'
                        logger.debug(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        # print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        self.display_positions()
                    else:
                        logger.error(f"Failed to place orders for {contract_symbol}.")
                        # print(f"Failed to place orders for {contract_symbol}.")
                        self.alert_active[contract_symbol] = False
                elif action == 'exit_long':
                    logger.info(f"{contract_symbol} - Long Exit detected. Closing open BUY position...")
                    print(f"{contract_symbol} - Long Exit detected. Closing open BUY position...")
                    self.initialize_contract(contract_symbol)
                    position_to_close = self.positions[contract_symbol]
                    if position_to_close['in_long_position']:
                        close_order_id = app.nextValidOrderId
                        quantity = 100
                        limit_price = close_price
                        profit_target_price = close_price - (100 / quantity)
                        stop_loss_price = close_price + (30 / quantity)

                        orders = app.place_bracket_order(contract, "SELL", quantity, limit_price, profit_target_price,
                                                         stop_loss_price, outside_rth=outside_rth)
                        if orders:
                            logger.info(f"Orders placed to close position for {contract_symbol}: {orders}")
                            print(f"Orders placed to close position for {contract_symbol}: {orders}")
                            # self.positions[contract_symbol]['status'] = 'PreSubmitted'
                            self.positions[contract_symbol]['in_long_position'] = False
                            sleep(2)
                        else:
                            logger.error(f"Failed to place exit orders for {contract_symbol}.")
                            print(f"Failed to place exit orders for {contract_symbol}.")
                            self.alert_active[contract_symbol] = False

                elif action == 'exit_short':
                    logger.info(f"{contract_symbol} - Short Exit detected. Closing open SELL position...")
                    print(f"{contract_symbol} - Short Exit detected. Closing open SELL position...")
                    self.initialize_contract(contract_symbol)
                    position_to_close = self.positions[contract_symbol]
                    if position_to_close['in_short_position']:
                        close_order_id = app.nextValidOrderId
                        quantity = 100
                        limit_price = close_price
                        profit_target_price = close_price + (100 / quantity)
                        stop_loss_price = close_price - (30 / quantity)

                        orders = app.place_bracket_order(contract, "BUY", quantity, limit_price, profit_target_price,
                                                         stop_loss_price, outside_rth=outside_rth)
                        if orders:
                            logger.info(f"Orders placed to close short position for {contract_symbol}: {orders}")
                            # print(f"Orders placed to close short position for {contract_symbol}: {orders}")
                            # Τοποθετούμε το position ως "Pre-Submitted" και θα ενημερωθεί με το status όταν γίνει filled
                            # self.positions[contract_symbol]['status'] = 'PreSubmitted'
                            self.positions[contract_symbol]['in_short_position'] = False
                            sleep(2)
                        else:
                            logger.error(f"Failed to place exit orders for {contract_symbol}.")
                            # print(f"Failed to place exit orders for {contract_symbol}.")
                            self.alert_active[contract_symbol] = False
                decision_flag.set()
            except Empty:
                logger.warning("Timeout waiting for signal from queue. No signals received.")
                print("Timeout waiting for signal from queue. No signals received.")
                continue
            except Exception as e:
                logger.exception(f"An unexpected error occurred: {str(e)}")
                print(f"An unexpected error occurred: {str(e)}")
                continue

    def wait_for_market_time(self):
        # print("Waiting")
        logging.info("Waiting for market time...")
        print("Checking market time...")
        est = pytz.timezone('America/New_York')
        now = datetime.now(est)
        # print(f"Current time {now}")
        market_open_time_naive = datetime(now.year, now.month, now.day, 9, 30)
        market_open_time = est.localize(market_open_time_naive)
        ten_minutes_after_open = market_open_time + timedelta(minutes=10)
        # print(f"Now: {now}")
        # print(f"Market open time: {market_open_time}")
        # print(f"10 minutes after open: {ten_minutes_after_open}")

        # if now >= ten_minutes_after_open:
        #     print("It's past 10 minutes after market open, proceeding...")
        # else:
        #     print("Waiting for 10 minutes after market open.")
        if now < ten_minutes_after_open:
            wait_seconds = (ten_minutes_after_open - now).total_seconds()
            wait_minutes = wait_seconds / 60

            logging.info(f"Waiting for {wait_minutes:.2f} minutes until 10 minutes after market open...")
            print(f"Waiting for {wait_minutes:.2f} minutes until 10 minutes after market open...")

            sleep(wait_seconds)

    # def handle_order_execution(self, orderId, status):
    #     # Ελέγχουμε αν το order είναι συνδεδεμένο με κάποιο position
    #     for contract_symbol, position in self.positions.items():
    #         if position['order_id'] == orderId:
    #             if status in ['Filled', 'Cancelled']:
    #                 print(f"Order {orderId} for {contract_symbol} is {status}. Closing position.")
    #                 self.positions[contract_symbol]['status'] = 'Closed'
    #                 self.positions[contract_symbol]['in_long_position'] = False
    #                 self.positions[contract_symbol]['in_short_position'] = False
    #                 self.positions[contract_symbol]['order_id'] = None
    #                 print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
    #                 self.alert_active[contract_symbol] = False  # Reset alert flag for new signals
    #             break

    # def handle_order_execution(self, orderId, status):
    #     """
    #     Αυτή η συνάρτηση διαχειρίζεται την ενημέρωση των θέσεων για κάθε μετοχή όταν αλλάζει το status των εντολών.
    #     """
    #     print(f"Processing order {orderId} with status: {status}")
    #
    #     # Ελέγχουμε αν το order είναι συνδεδεμένο με κάποιο position για συγκεκριμένο symbol
    #     for contract_symbol, position in self.positions.items():
    #         if position['order_id'] == orderId:
    #             print(f"Found order {orderId} for contract {contract_symbol} with status: {status}")
    #
    #             if status in ['Pre-Submitted', 'Submitted']:
    #                 # Αν η εντολή είναι Pre-Submitted ή Submitted, ενημερώνουμε ότι το position είναι ακόμα ανοιχτό
    #                 print(f"Order {orderId} for {contract_symbol} is {status}. Keeping position open.")
    #                 self.positions[contract_symbol]['status'] = 'Open'
    #                 # Δεν χρειάζεται να κλείσουμε τίποτα ακόμα
    #
    #             elif status in ['Filled', 'Cancelled']:
    #                 # Αν το status είναι Filled ή Cancelled, κλείνουμε το position
    #                 print(f"Order {orderId} for {contract_symbol} is {status}. Closing position.")
    #                 self.positions[contract_symbol]['status'] = 'Closed'
    #                 self.positions[contract_symbol]['in_long_position'] = False
    #                 self.positions[contract_symbol]['in_short_position'] = False
    #                 self.positions[contract_symbol]['order_id'] = None
    #                 print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
    #
    #             else:
    #                 print(f"Order {orderId} for {contract_symbol} has status {status}. No action taken.")
    #
    #             break

    def handle_order_execution(self, orderId, status):
        # with self.lock:
            # Ελέγχουμε αν το order συνδέεται με κάποιο position
        for contract_symbol, position in self.positions.items():
            if position['order_id'] == orderId:
                logger.info(f"Processing order {orderId} for {contract_symbol} with status: {status}")
                # print(f"Processing order {orderId} for {contract_symbol} with status: {status}")

                # if status in ['PreSubmitted', 'PendingSubmit']:
                #     # Αν το status είναι Pre-Submitted ή Submitted, κρατάμε την θέση ανοιχτή
                #     self.positions[contract_symbol]['status'] = 'Locked'
                #     print(f"Order {orderId} for {contract_symbol} is still in {status}. Position locked.")
                # elif status in ['Submitted']:
                #     self.positions[contract_symbol]['status'] = 'Open'
                #     print(f"Order {orderId} for {contract_symbol} is still in {status}. Position open.")

                if status in ['PreSubmitted', 'PendingSubmit', 'Submitted']:
                    # Καθορισμός της θέσης ως ανοιχτή (αμέσως μόλις η εντολή τοποθετείται)
                    if 'BUY' in orderId:
                        self.positions[contract_symbol]['in_long_position'] = True
                        self.positions[contract_symbol]['in_short_position'] = False
                    elif 'SELL' in orderId:
                        self.positions[contract_symbol]['in_short_position'] = True
                        self.positions[contract_symbol]['in_long_position'] = False

                    self.positions[contract_symbol]['status'] = 'Locked' if status in ['PreSubmitted',
                                                                                       'PendingSubmit'] else 'Open'
                    logger.info(f"Order {orderId} for {contract_symbol} is still in {status}. Position locked/open.")
                    # print(f"Order {orderId} for {contract_symbol} is still in {status}. Position locked/open.")

                elif status in ['Filled', 'Cancelled', 'Ιnactive']:
                    self.positions[contract_symbol]['status'] = 'Closed'
                    self.positions[contract_symbol]['in_long_position'] = False
                    self.positions[contract_symbol]['in_short_position'] = False
                    self.positions[contract_symbol]['order_id'] = None
                    self.alert_active[contract_symbol] = False
                    logger.info(f"Order {orderId} for {contract_symbol} is {status}. Position closed.")
                    # print(f"Order {orderId} for {contract_symbol} is {status}. Position closed.")
                break

    def display_positions(self):
        logger.info("--- Positions List ---")
        print("\n--- Positions List ---")
        for contract_symbol, pos in self.positions.items():
            logger.info(f"Contract: {contract_symbol}, Position Type: {'Long' if pos['in_long_position'] else 'Short'}, "
                        f"Order ID: {pos['order_id']}, Status: {pos['status']}")
            print(f"Contract: {contract_symbol}, Position Type: {'Long' if pos['in_long_position'] else 'Short'}, "
                  f"Order ID: {pos['order_id']}, Status: {pos['status']}")
        logger.info("----------------------")
        print("----------------------")

    # def display_orders(self):
    #     orders = list(self.order_queue.queue)
    #     for order in orders:
    #         print(f"Order ID: {order.orderId}, Action: {order.action}, Quantity: {order.totalQuantity}")
    # ena antistoixo pou tha vlepw mono ta positions

    # def stop_program_after_duration(duration, decision_flag):
    #     #     sleep(duration)
    #     #     decision_flag.set()
    #     #     print("Program is stopping after the set duration.")

    # @staticmethod
    # def terminate_program(self):
    #     print("2 ώρες έχουν περάσει. Τερματισμός προγράμματος.")
    #     sys.exit(0)

    def listen_for_esc(self, decision_queue, stop_flag):
        while True:
            # na prosthesw to close order
            keyboard.wait('esc')
            print("\nESC pressed. Select an option:")
            print("1. Cancel an order")
            print("2. Continue current program")
            print("3. Return to main menu")
            choice = input("Enter 1, 2, or 3: ")

            if choice == '1':
                # self.display_orders()
                order_id = input("Enter the Order ID to cancel: ")
                try:
                    from ib_api import IBApi
                    from data_processing import DataProcessor
                    from database import Database

                    db = Database()
                    data_processor = DataProcessor(db, api_helper)
                    app = IBApi(data_processor, db)

                    order_id = int(order_id)
                    app.cancel_open_order(order_id)
                    logger.info(f"Order {order_id} has been cancelled.")
                    # print(f"Order {order_id} has been cancelled.")
                except ValueError:
                    logger.error("Invalid Order ID. Please enter a numeric value.")
                    print("Invalid Order ID. Please enter a numeric value.")
            elif choice == '2':
                decision_queue.put('continue', None, None)
            elif choice == '3':
                decision_queue.put('menu', None, None)
                stop_flag.set()
            else:
                logger.warning("Invalid choice. Please try again.")
                print("Invalid choice. Please try again.")
