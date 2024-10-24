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
    def __init__(self, api_helper):
        self.in_long_position = False
        self.in_short_position = False
        self.place_orders_outside_rth = False
        self.alert_active = {}
        self.positions = {}
        self.active_positions = []
        self.lock = threading.Lock()
        self.api_helper = api_helper

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
                    continue
                elif choice == 'menu':
                    logger.info("Returning to the main menu.")
                    print("Returning to the main menu.")
                    return
                else:
                    logger.warning("Invalid choice. Please try again.")
                    # print("Invalid choice. Please try again.")
            else:
                logger.warning("Invalid input. Please enter 'yes' or 'no'.")
                # print("Invalid input. Please enter 'yes' or 'no'.")

    # def is_market_open(self):
    #     now = datetime.now(pytz.timezone('America/New_York'))
    #     logger.info(f"Current time in NY: {now}")
    #     # print(f"Current time in NY: {now}")
    #     market_open = time(9, 30)
    #     market_close = time(16, 0)
    #     if now.weekday() >= 5:  # If it's Saturday(5) or Sunday(6)
    #         logger.info("Market is closed because it's the weekend.")
    #         return False
    #     if market_open <= now.time() <= market_close:
    #         logger.info("Market is currently open.")
    #         print("Market is currently open.")
    #         return True
    #     else:
    #         logger.info("Market is closed at this time.")
    #         print("Market is closed at this time.")
    #         return False

    # def get_order_details_from_user(self, action):
    #     quantity = int(input(f"Enter quantity for {action} order: "))
    #     limit_price = float(input(f"Enter limit price for {action} order: "))
    #     profit_target = float(input(f"Enter profit target for {action} order: "))
    #     stop_loss = float(input(f"Enter stop loss for {action} order: "))
    #     logger.info(f"Order details for {action}: quantity={quantity}, limit_price={limit_price}, profit_target={profit_target}, stop_loss={stop_loss}")
    #     return quantity, limit_price, profit_target, stop_loss

    # def check_and_place_order(self, contract, action, quantity, limit_price, profit_target, stop_loss):
    #     import data_processing
    #     import database
    #     import ib_api
    #     db = database.Database
    #     data_processor = data_processing.DataProcessor(db)
    #     app = ib_api.IBApi(data_processor, db)
    #
    #     if self.place_orders_outside_rth or self.is_market_open():
    #         app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss,
    #                                      outside_rth=self.place_orders_outside_rth)
    #         logger.info(f"Placed {action} order for {quantity} shares at {limit_price}")
    #         # print(f"Placed {action} order for {quantity} shares at {limit_price}")
    #     else:
    #         logger.warning("Market is closed and order placing outside RTH is not allowed.")
    #         # print("Market is closed and order placing outside RTH is not allowed.")

    # def send_alert(self, message):
    #     logger.info(f"ALERT: {message}")
    #     # print(f"ALERT: {message}")

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
        for position in reversed(self.positions):
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
        if not self.wait_for_market_time():
            logger.info(f"Skipping order placement for {contract_symbol} because market is not open.")
            print(f"Skipping order placement for {contract_symbol} because market is not open.")
            return        #
        # if not isinstance(df_entry, pd.DataFrame):
        #     logger.info(f"Error: df_entry is not a DataFrame but a {type(df_entry)}")
        #     print(f"Error: df_entry is not a DataFrame but a {type(df_entry)}")
        #     return
        # if not isinstance(df_exit, pd.DataFrame):
        #     logger.info(f"Error: df_exit is not a DataFrame but a {type(df_exit)}")
        #     print(f"Error: df_exit is not a DataFrame but a {type(df_exit)}")
        #     return

        if self.alert_active.get(contract_symbol, False):
            logger.info(f"Alert already active for {contract_symbol}, skipping.")
            print(f"Alert already active for {contract_symbol}, skipping.")
            return

        try:
            df_entry['Date'] = pd.to_datetime(df_entry['Date'], errors='coerce').dt.tz_localize(None)
            df_exit['Date'] = pd.to_datetime(df_exit['Date'], errors='coerce').dt.tz_localize(None)

            i = len(df_entry) - 1
            timestamp = df_entry.iloc[i]['Date']

            print(f"Original timestamp: {timestamp}, Timezone: {timestamp.tzinfo}")

            if timestamp.tzinfo is None:
                logger.info(f"Localizing timestamp {timestamp} to America/New_York.")
                timestamp = pytz.timezone('America/New_York').localize(timestamp)
            else:
                logger.info(f"Converting timestamp {timestamp} to America/New_York timezone.")
                timestamp = timestamp.astimezone(pytz.timezone('America/New_York'))

            print(f"Localized timestamp: {timestamp}, Timezone: {timestamp.tzinfo}")

            est = pytz.timezone('America/New_York')
            nine_forty_am = est.localize(datetime(timestamp.year, timestamp.month, timestamp.day, 9, 40))

            if timestamp < nine_forty_am:
                logger.info(
                    f"Skipping signals for {contract_symbol} because the timestamp {timestamp} is before 9:40 AM.")
                print(f"Skipping signals for {contract_symbol} because the timestamp {timestamp} is before 9:40 AM.")
                return

            # long_entry = df_entry.at[i, 'Long_Entry'] if 'Long_Entry' in df_entry.columns else False
            # short_entry = df_entry.at[i, 'Short_Entry'] if 'Short_Entry' in df_entry.columns else False
            #
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

            if (long_entry and long_exit) or (short_entry and short_exit):
                logger.warning(f"Conflicting signals for {contract_symbol}. No action taken.")
                print(f"Conflicting signals for {contract_symbol}. No action taken.")
                # continue
                self.alert_active[contract_symbol] = False
                return

            if long_entry and short_entry:
                logger.warning(f"Both Long Entry and Short Entry signals present for {contract_symbol}. No action taken.")
                print(
                    f"Both Long Entry and Short Entry signals present for {contract_symbol}. No action taken.")
                # continue
                self.alert_active[contract_symbol] = False
                return

            if long_entry and not self.positions[contract_symbol]['in_long_position'] and not self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'long', i, previous_close, timestamp))
                logger.info(f"Long entry signal detected: {signals[-1]}")
                print(f"Long entry signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            if short_entry and not self.positions[contract_symbol]['in_long_position'] and not self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'short', i, previous_close, timestamp))
                logger.info(f"Short entry signal detected: {signals[-1]}")
                print(f"Short entry signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            if long_exit and self.positions[contract_symbol]['in_long_position']:
                signals.append((contract_symbol, 'exit_long', i, previous_close, timestamp))
                logger.info(f"Long exit signal detected: {signals[-1]}")
                print(f"Long exit signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break

            if short_exit and self.positions[contract_symbol]['in_short_position']:
                signals.append((contract_symbol, 'exit_short', i, previous_close, timestamp))
                logger.info(f"Short exit signal detected: {signals[-1]}")
                print(f"Short exit signal detected: {signals[-1]}")
                self.alert_active[contract_symbol] = True
                # break
        except KeyError as e:
            logger.info(f"KeyError at index {i}: {e}, skipping this index.")
            print(f"KeyError at index {i}: {e}, skipping this index.")
            # continue
        except Exception as e:
            logger.info(f"Unexpected error at index {i}: {e}, skipping this index.")
            print(f"Unexpected error at index {i}: {e}, skipping this index.")
            # continue

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
            try:
                logger.info("Waiting for signal from queue...")
                print("Waiting for signal from queue...")
                signal = decision_queue.get(timeout=30)
                logger.info(f"Signal received from queue: {signal}")
                print(f"Signal received from queue: {signal}")

                # self.wait_for_market_time()

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

                # Long Entry
                if action == 'long':
                    logger.info(f"Initializing contract for long position: {contract_symbol}")
                    self.initialize_contract(contract_symbol)

                    quantity = 100
                    limit_price = close_price - (30 / quantity)
                    profit_target_price = close_price + (100 / quantity)
                    stop_loss_price = close_price - (61 / quantity)

                    orders = app.place_bracket_order(contract, "BUY", quantity, limit_price, profit_target_price,
                                                     stop_loss_price,
                                                     outside_rth=outside_rth)
                    if orders:
                        logger.info(f"Orders placed for {contract_symbol}: {orders}")
                        print(f"Orders placed for {contract_symbol}: {orders}")
                        self.positions[contract_symbol]['in_long_position'] = True
                        self.positions[contract_symbol]['in_short_position'] = False
                        self.positions[contract_symbol]['order_id'] = orders[0].orderId
                        sleep(2)
                        # self.positions[contract_symbol]['status'] = 'PreSubmitted'
                        logger.info(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        # print(f"Updated positions for {contract_symbol}: {self.positions[contract_symbol]}")
                        self.display_positions()
                    else:
                        logger.error(f"Failed to place orders for {contract_symbol}.")
                        print(f"Failed to place orders for {contract_symbol}.")
                        self.alert_active[contract_symbol] = False

                # Short Entry
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

                # Exit Long Position
                elif action == 'exit_long':
                    logger.info(
                        f"{contract_symbol} - Long Exit detected. Closing open BUY position with market order...")
                    print(f"{contract_symbol} - Long Exit detected. Closing open BUY position with market order...")

                    app.cancel_open_order(self.positions[contract_symbol]['order_id'])

                    position_to_close = self.positions[contract_symbol]
                    if position_to_close['in_long_position']:
                        quantity = 100

                        market_order = app.create_order(app.nextValidOrderId, "SELL", "MKT", quantity)
                        app.placeOrder(app.nextValidOrderId, contract, market_order)
                        app.nextValidOrderId += 1

                        logger.info(f"Market order placed to close long position for {contract_symbol}")
                        print(f"Market order placed to close long position for {contract_symbol}")

                        self.positions[contract_symbol]['in_long_position'] = False
                        self.positions[contract_symbol]['order_id'] = None
                        sleep(2)
                    else:
                        logger.error(f"No open long position for {contract_symbol} to close.")
                        print(f"No open long position for {contract_symbol} to close.")

                # Exit Short Position
                elif action == 'exit_short':
                    logger.info(
                        f"{contract_symbol} - Short Exit detected. Closing open SELL position with market order...")
                    print(f"{contract_symbol} - Short Exit detected. Closing open SELL position with market order...")

                    position_to_close = self.positions[contract_symbol]
                    if position_to_close['in_short_position']:
                        order_id = position_to_close['order_id']
                        app.cancel_open_order(order_id)

                        quantity = 100

                        market_order = app.create_order(app.nextValidOrderId, "BUY", "MKT", quantity)
                        app.placeOrder(app.nextValidOrderId, contract, market_order)
                        app.nextValidOrderId += 1

                        logger.info(f"Market order placed to close short position for {contract_symbol}")
                        print(f"Market order placed to close short position for {contract_symbol}")

                        self.positions[contract_symbol]['in_short_position'] = False
                        self.positions[contract_symbol]['order_id'] = None
                        sleep(2)
                    else:
                        logger.error(f"No open short position for {contract_symbol} to close.")
                        print(f"No open short position for {contract_symbol} to close.")

                decision_flag.set()

            except Empty:
                logger.info("Timeout waiting for signal from queue. No signals received.")
                print("Timeout waiting for signal from queue. No signals received.")
                continue

            except Exception as e:
                logger.exception(f"An unexpected error occurred: {str(e)}")
                print(f"An unexpected error occurred: {str(e)}")
                continue


    def wait_for_market_time(self):
        logging.info("Checking for market time...")
        print("Checking market time...")

        # Set up timezone for Eastern Time (New York)
        est = pytz.timezone('America/New_York')
        now = datetime.now(est)

        # Market open time (9:30 AM) and market close time (4:00 PM)
        market_open_time_naive = datetime(now.year, now.month, now.day, 9, 30)
        market_close_time_naive = datetime(now.year, now.month, now.day, 16, 20)
        closing_time = datetime(now.year, now.month, now.day, 15, 55)

        # Localize the times to EST timezone
        market_open_time = est.localize(market_open_time_naive)
        market_close_time = est.localize(market_close_time_naive)
        closing_time_est = est.localize(closing_time)

        ten_minutes_after_open = market_open_time + timedelta(minutes=10)

        if now < ten_minutes_after_open:
            wait_seconds = (ten_minutes_after_open - now).total_seconds()
            wait_minutes = wait_seconds / 60
            logging.info(f"Waiting for {wait_minutes:.2f} minutes until 10 minutes after market open...")
            print(f"Waiting for {wait_minutes:.2f} minutes until 10 minutes after market open...")
            sleep(wait_seconds)
            return False

        # elif now >= closing_time_est:
        #     self.close_open_positions_and_cancel_orders()
        #     logging.info("It is 15:55. All positions closed and orders cancelled.")
        #     print("It is 15:55. All positions closed and orders cancelled.")
        #
        #     # Calculate the next market open time
        #     next_market_open_time = market_open_time_naive + timedelta(days=1)
        #     next_market_open_time = est.localize(next_market_open_time)
        #     wait_seconds = (next_market_open_time - now).total_seconds()
        #     wait_hours = wait_seconds / 3600
        #     logging.info(f"Waiting {wait_hours:.2f} hours until next market open...")
        #     print(f"Waiting {wait_hours:.2f} hours until next market open...")
        #     sleep(wait_seconds)
        #     return False

        elif now >= market_open_time and now < market_close_time:
            logging.info("Market is open. Proceeding with orders.")
            print("Market is open. Proceeding with orders.")
            return True

    def handle_order_execution(self, orderId, status):
        # with self.lock:
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

    # def stop_program_after_duration(duration, decision_flag):
    #     #     sleep(duration)
    #     #     decision_flag.set()
    #     #     print("Program is stopping after the set duration.")

    # @staticmethod
    # def terminate_program(self):
    #     print("2 ώρες έχουν περάσει. Τερματισμός προγράμματος.")
    #     sys.exit(0)

    def close_open_positions_and_cancel_orders(self):
        # from ib_api import IBApi
        # from data_processing import DataProcessor
        # from database import Database
        #
        # db = Database()
        # data_processor = DataProcessor(db, api_helper)
        # app = IBApi(data_processor, db)
        logging.info("Closing all open positions and cancelling unfilled orders at market close.")
        print("Closing all open positions and cancelling unfilled orders at market close.")

        open_positions = self.api_helper.get_open_positions()
        for position in open_positions:
            contract = position.contract
            action = "SELL" if position.position > 0 else "BUY"
            quantity = abs(position.position)

            close_order = self.api_helper.create_order(self.api_helper.app.nextValidOrderId, action, "MKT", quantity)
            self.api_helper.placeOrder(self.api_helper.app.nextValidOrderId, contract, close_order)
            self.api_helper.nextValidOrderId += 1

            logging.info(f"Closed position for {contract.symbol} with action {action} and quantity {quantity}.")
            print(f"Closed position for {contract.symbol} with action {action} and quantity {quantity}.")

        open_orders = self.api_helper.get_open_orders()
        for order in open_orders:
            self.api_helper.cancel_open_order(order.orderId)
            logging.info(f"Cancelled unfilled order ID {order.orderId} for {order.contract.symbol}.")
            print(f"Cancelled unfilled order ID {order.orderId} for {order.contract.symbol}.")

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

