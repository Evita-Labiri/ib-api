from datetime import datetime, time
from time import sleep
import pytz
from globals import decision_queue, stop_flag
import keyboard

class OrderManager:
    def __init__(self, app):
        self.app = app
        self.in_long_position = False
        self.in_short_position = False
        self.place_orders_outside_rth = False

    def set_order_outside_rth(self):
        outside_rth_input = input(f"Do you want to place orders outside regular trading hours? (yes/no): ").strip().lower()
        self.place_orders_outside_rth = outside_rth_input == 'yes'

    def is_market_open(self):
        now = datetime.now(pytz.timezone('America/New_York'))
        market_open = time(9, 30)
        market_close = time(16, 0)
        if now.weekday() >= 5:  # If it's Saturday(5) or Sunday(6)
            return False
        return market_open <= now.time() <= market_close

    def get_order_details_from_user(self, action):
        quantity = int(input(f"Enter quantity for {action} order: "))
        limit_price = float(input(f"Enter limit price for {action} order: "))
        profit_target = float(input(f"Enter profit target for {action} order: "))
        stop_loss = float(input(f"Enter stop loss for {action} order: "))
        return quantity, limit_price, profit_target, stop_loss

    def check_and_place_order(self, contract, action, quantity, limit_price, profit_target, stop_loss):
        if self.place_orders_outside_rth or self.is_market_open():
            self.app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss,
                                         outsideRth=self.place_orders_outside_rth)
            print(f"Placed {action} order for {quantity} shares at {limit_price}")
        else:
            print("Market is closed and order placing outside RTH is not allowed.")

    def send_alert(self, message):
        print(f"ALERT: {message}")

    # def get_user_choice(self, message):
    #     while True:
    #         choice = input(message).lower()
    #         if choice in ['yes', 'no']:
    #             return choice
    #         print("Invalid input. Please enter 'yes' or 'no'.")

    # def wait_for_user_decision(self, message):
    #     while True:
    #         choice = input(message).lower()
    #         if choice in ['yes', 'no']:
    #             return choice
    #         print("Invalid input. Please enter 'yes' or 'no'.")
    #         sleep(1)

    # def process_signals_and_place_orders(self, df, contract):
    #     for i in range(len(df)):
    #         try:
    #             if df.at[i, 'Long_Entry'] and not self.in_long_position:
    #                 self.send_alert("Long Entry detected!")
    #                 user_choice = self.get_user_choice("Long Entry detected. Do you want to place an order? (yes/no): ")
    #
    #                 if user_choice == 'yes':
    #                     limit_price = df.at[i, 'Close']
    #                     profit_target = limit_price * 1.02  # Προκαθορισμένο 2% κέρδος
    #                     stop_loss = limit_price * 0.98  # Προκαθορισμένη 2% απώλεια
    #
    #                     user_limit_price = input(f"Enter limit price (default: {limit_price}): ")
    #                     user_profit_target = input(f"Enter profit target price (default: {profit_target}): ")
    #                     user_stop_loss = input(f"Enter stop loss price (default: {stop_loss}): ")
    #
    #                     limit_price = float(user_limit_price) if user_limit_price else limit_price
    #                     profit_target = float(user_profit_target) if user_profit_target else profit_target
    #                     stop_loss = float(user_stop_loss) if user_stop_loss else stop_loss
    #
    #                     self.check_and_place_order(contract, "BUY", 100, limit_price, profit_target, stop_loss)
    #                     self.in_long_position = True
    #                     self.in_short_position = False
    #                     print(f"Placed BUY bracket order at index {i}")
    #                 else:
    #                     print("User chose not to place a Long Entry order.")
    #
    #             elif df.at[i, 'Short_Entry'] and not self.in_short_position:
    #                 self.send_alert("Short Entry detected!")
    #                 user_choice = self.get_user_choice(
    #                     "Short Entry detected. Do you want to place an order? (yes/no): ")
    #
    #                 if user_choice == 'yes':
    #                     limit_price = df.at[i, 'Close']
    #                     profit_target = limit_price * 0.98  # Προκαθορισμένο 2% κέρδος
    #                     stop_loss = limit_price * 1.02  # Προκαθορισμένη 2% απώλεια
    #
    #                     user_limit_price = input(f"Enter limit price (default: {limit_price}): ")
    #                     user_profit_target = input(f"Enter profit target price (default: {profit_target}): ")
    #                     user_stop_loss = input(f"Enter stop loss price (default: {stop_loss}): ")
    #
    #                     limit_price = float(user_limit_price) if user_limit_price else limit_price
    #                     profit_target = float(user_profit_target) if user_profit_target else profit_target
    #                     stop_loss = float(user_stop_loss) if user_stop_loss else stop_loss
    #
    #                     self.check_and_place_order(contract, "SELL", 100, limit_price, profit_target, stop_loss)
    #                     self.in_short_position = True
    #                     self.in_long_position = False
    #                     print(f"Placed SELL bracket order at index {i}")
    #                 else:
    #                     print("User chose not to place a Short Entry order.")
    #
    #             elif df.at[i, 'Long_Exit'] and self.in_long_position:
    #                 self.send_alert("Long Exit detected!")
    #                 user_choice = self.get_user_choice(
    #                     "Long Exit detected. Do you want to cancel the order? (yes/no): ")
    #
    #                 if user_choice == 'yes':
    #                     self.app.cancel_open_order(self.app.nextValidOrderId - 1)
    #                     self.in_long_position = False
    #                     print(f"Cancelled BUY order (closing long position) at index {i}")
    #                 else:
    #                     print("User chose not to cancel the Long Exit order.")
    #
    #             elif df.at[i, 'Short_Exit'] and self.in_short_position:
    #                 self.send_alert("Short Exit detected!")
    #                 user_choice = self.get_user_choice(
    #                     "Short Exit detected. Do you want to cancel the order? (yes/no): ")
    #
    #                 if user_choice == 'yes':
    #                     self.app.cancel_open_order(self.app.nextValidOrderId - 1)
    #                     self.in_short_position = False
    #                     print(f"Cancelled SELL order (closing short position) at index {i}")
    #                 else:
    #                     print("User chose not to cancel the Short Exit order.")
    #
    #         except KeyError as e:
    #             print(f"KeyError at index {i}: {e}, skipping this index.")
    #             continue
    #         except Exception as e:
    #             print(f"Unexpected error at index {i}: {e}, skipping this index.")
    #             continue

    # def handle_user_decision(self, order_manager, df, index, contract, direction):
    #     try:
    #         limit_price = df.at[index, 'Close']
    #         profit_target = limit_price * (1.02 if direction == "BUY" else 0.98)
    #         stop_loss = limit_price * (0.98 if direction == "BUY" else 1.02)
    #
    #         user_limit_price = input(
    #             f"{direction} Entry detected at index {index}. Enter limit price (default: {limit_price}): ")
    #         user_profit_target = input(f"Enter profit target price (default: {profit_target}): ")
    #         user_stop_loss = input(f"Enter stop loss price (default: {stop_loss}): ")
    #
    #         limit_price = float(user_limit_price) if user_limit_price else limit_price
    #         profit_target = float(user_profit_target) if user_profit_target else profit_target
    #         stop_loss = float(user_stop_loss) if user_stop_loss else stop_loss
    #
    #         order_manager.check_and_place_order(contract, direction, 100, limit_price, profit_target, stop_loss)
    #         return True
    #     except Exception as e:
    #         print(f"An error occurred while handling user decision: {e}")
    #         return False

    # def process_signals_and_place_orders(self, df, contract):
    #     for i in range(len(df)):
    #         try:
    #             if df.at[i, 'Long_Entry'] and not self.in_long_position:
    #                 stop_flag.set()
    #                 decision_queue.put(('long', i))
    #                 stop_flag.wait()  # Wait for user decision
    #                 stop_flag.clear()
    #
    #             elif df.at[i, 'Short_Entry'] and not self.in_short_position:
    #                 stop_flag.set()
    #                 decision_queue.put(('short', i))
    #                 stop_flag.wait()  # Wait for user decision
    #                 stop_flag.clear()
    #
    #             elif df.at[i, 'Long_Exit'] and self.in_long_position:
    #                 decision_queue.put(('exit_long', i))
    #
    #             elif df.at[i, 'Short_Exit'] and self.in_short_position:
    #                 decision_queue.put(('exit_short', i))
    #
    #         except KeyError as e:
    #             print(f"KeyError at index {i}: {e}, skipping this index.")
    #             continue
    #         except Exception as e:
    #             print(f"Unexpected error at index {i}: {e}, skipping this index.")
    #             continue
    def process_signals_and_place_orders(self, df, contract, decision_queue, stop_flag):
        signals = []

        for i in range(len(df)):
            try:
                if df.at[i, 'Long_Entry'] and not self.in_long_position:
                    signals.append(('long', i, contract))

                elif df.at[i, 'Short_Entry'] and not self.in_short_position:
                    signals.append(('short', i, contract))

                elif df.at[i, 'Long_Exit'] and self.in_long_position:
                    signals.append(('exit_long', i, contract))

                elif df.at[i, 'Short_Exit'] and self.in_short_position:
                    signals.append(('exit_short', i, contract))

            except KeyError as e:
                print(f"KeyError at index {i}: {e}, skipping this index.")
                continue
            except Exception as e:
                print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue

        for signal in signals:
            stop_flag.set()
            decision_queue.put(signal)
            stop_flag.wait()  # Wait for user decision
            stop_flag.clear()

    def handle_decision(self, app, decision_queue, stop_flag):
        while True:
            signal = decision_queue.get()
            if signal == 'exit':
                break

            action, index, contract = signal
            if action == 'long':
                limit_price = float(input(f"Long Entry detected at index {index}. Enter limit price: "))
                profit_target = limit_price * 1.02
                stop_loss = limit_price * 0.98
                app.place_bracket_order(contract, "BUY", 100, limit_price, profit_target, stop_loss)
                self.in_long_position = True
                self.in_short_position = False
            elif action == 'short':
                limit_price = float(input(f"Short Entry detected at index {index}. Enter limit price: "))
                profit_target = limit_price * 0.98
                stop_loss = limit_price * 1.02
                app.place_bracket_order(contract, "SELL", 100, limit_price, profit_target, stop_loss)
                self.in_short_position = True
                self.in_long_position = False
            elif action == 'exit_long':
                app.cancel_open_order(app.nextValidOrderId - 1)
                self.in_long_position = False
            elif action == 'exit_short':
                app.cancel_open_order(app.nextValidOrderId - 1)
                self.in_short_position = False

            stop_flag.set()  # Allow main thread to continue
            print("2. Continue current program")
            print("3. Return to main menu")
            choice = input("Enter 1, 2, or 3: ")

            if choice == '1':
                decision_queue.put(('cancel', None, None))
            elif choice == '2':
                decision_queue.put(('continue', None, None))
            elif choice == '3':
                decision_queue.put(('menu', None, None))
                stop_flag.set()
            else:
                print("Invalid choice. Please try again.")
            sleep(1)

    # def display_orders(self):
    #     orders = list(self.app.order_queue.queue)
    #     for order in orders:
    #         print(f"Order ID: {order.orderId}, Action: {order.action}, Quantity: {order.totalQuantity}")

    def listen_for_esc(self, decision_queue, stop_flag):
        while True:
            keyboard.wait('esc')
            print("\nESC pressed. Select an option:")
            print("1. Cancel an order")
            print("2. Continue current program")
            print("3. Return to main menu")
            choice = input("Enter 1, 2, or 3: ")

            if choice == '1':
                decision_queue.put('cancel',  None, None)
            elif choice == '2':
                decision_queue.put('continue', None, None)
            elif choice == '3':
                decision_queue.put('menu', None, None)
                stop_flag.set()
            else:
                print("Invalid choice. Please try again.")
