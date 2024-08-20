from datetime import datetime, time
from time import sleep
import pytz
import keyboard


class OrderManager:
    def __init__(self):
        # self.app = app
        self.in_long_position = False
        self.in_short_position = False
        self.place_orders_outside_rth = False
        self.alert_active = False
        self.positions = []

    def set_order_outside_rth(self):
        while True:
            # Ρωτάμε τον χρήστη αν θέλει να επιτρέψει την εκτέλεση εντολών εκτός RTH
            outside_rth_input = input(
                "Do you want to place orders outside regular trading hours? (yes/no): ").strip().lower()
            if outside_rth_input == 'yes':
                self.place_orders_outside_rth = True
                break
            elif outside_rth_input == 'no':
                print("Orders outside regular trading hours are not allowed.")
                # Δίνουμε επιλογές στον χρήστη για να προσπαθήσει ξανά ή να επιστρέψει στο κεντρικό μενού
                choice = input("Would you like to retry or return to the main menu? (retry/menu): ").strip().lower()
                if choice == 'retry':
                    continue  # Επιστρέφει στην αρχή της while loop για να προσπαθήσει ξανά
                elif choice == 'menu':
                    print("Returning to the main menu.")
                    return  # Επιστρέφει στο κεντρικό μενού (πρέπει να χειριστείς το επιστροφή στο μενού εκτός της συνάρτησης)
                else:
                    print("Invalid choice. Please try again.")
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")

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
        import data_processing
        import database
        import ib_api
        db = database.Database
        data_processor = data_processing.DataProcessor
        app = ib_api.IBApi(data_processor, db)

        if self.place_orders_outside_rth or self.is_market_open():
            app.place_bracket_order(contract, action, quantity, limit_price, profit_target, stop_loss,
                                         outside_rth=self.place_orders_outside_rth)
            print(f"Placed {action} order for {quantity} shares at {limit_price}")
        else:
            print("Market is closed and order placing outside RTH is not allowed.")

    def send_alert(self, message):
        print(f"ALERT: {message}")

    # def wait_for_user_decision(self, message):
    #     while True:
    #         choice = input(message).lower()
    #         if choice in ['yes', 'no']:
    #             return choice
    #         print("Invalid input. Please enter 'yes' or 'no'.")
    #         sleep(1)


    def add_position(self, position_type, order_id, status="Open"):
        position = {
            'type': position_type,
            'order_id': order_id,
            'status': status
        }
        self.positions.append(position)

    def update_position_status(self, order_id, new_status):
        for position in self.positions:
            if position['order_id'] == order_id:
                position['status'] = new_status
                break

    def display_positions(self):
        if not self.positions:
            print("No positions available.")
            return

        print("\n--- Positions List ---")
        for position in self.positions:
            print(
                f"Position Type: {position['type']}, Order ID: {position['order_id']}, Status: {position['status']}")
        print("----------------------\n")

    def process_signals_and_place_orders(self, df, contract, decision_queue, stop_flag):
        signals = []

        if self.alert_active:
            return

        for i in range(len(df)):
            try:
                if df.at[i, 'Long_Entry'] and not self.in_long_position and not self.in_short_position:
                    self.send_alert("Long Entry Position Open")
                    signals.append(('long', i, contract, df.at[i, 'Close']))
                    self.alert_active = True
                    break

                elif df.at[i, 'Short_Entry'] and not self.in_long_position and not self.in_short_position:
                    self.send_alert("Short Entry Position Open")
                    signals.append(('short', i, contract, df.at[i, 'Close']))
                    self.alert_active = True
                    break

                # elif df.at[i, 'Long_Exit'] and self.in_long_position:
                #     self.send_alert("Long Exit Position Open")
                #     signals.append(('exit_long', i, contract, df.at[i, 'Close']))
                #     self.alert_active = True
                #     break
                #
                # elif df.at[i, 'Short_Exit'] and self.in_short_position:
                #     self.send_alert("Short Exit Position Open")
                #     signals.append(('exit_short', i, contract, df.at[i, 'Close']))
                #     self.alert_active = True
                #     break

            except KeyError as e:
                print(f"KeyError at index {i}: {e}, skipping this index.")
                continue
            except Exception as e:
                print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue

        for signal in signals:
            stop_flag.set()
            decision_queue.put(signal)
            stop_flag.wait()
            stop_flag.clear()
            self.alert_active = False  # Reset alert flag after processing
            self.place_orders_outside_rth = False

    def handle_decision(self, app, decision_queue, stop_flag):
        while True:
            signal = decision_queue.get()
            if signal == 'exit':
                break

            action, index, contract, close_price = signal

            # print(
            #     f"[DEBUG] Handling decision - action: {action}, in_long_position: {self.in_long_position}, in_short_position: {self.in_short_position}, alert_active: {self.alert_active}")

            while app.nextValidOrderId is None:
                print("Waiting for next valid order ID...")
                sleep(1)

            outside_rth = self.place_orders_outside_rth

            if action == 'long':
                app.place_bracket_order(contract, "BUY", 100, close_price, close_price + 100, close_price - 50,
                                        outside_rth=outside_rth)
                self.in_long_position = True
                self.in_short_position = False
                self.add_position('Long', app.nextValidOrderId, 'Open')
                self.display_positions()

                # print(
                #     f"[DEBUG] Long position opened - in_long_position: {self.in_long_position}, in_short_position: {self.in_short_position}")

            elif action == 'short':
                app.place_bracket_order(contract, "SELL", 100, close_price, close_price - 100, close_price + 50,
                                        outside_rth=outside_rth)

                self.in_short_position = True
                self.in_long_position = False
                # print(
                #     f"[DEBUG] Short position opened - in_short_position: {self.in_short_position}, in_long_position: {self.in_long_position}")
                self.add_position('Short', app.nextValidOrderId, 'Open')
                self.display_positions()

            # elif action == 'exit_long':
            #     print(f"Long Exit detected at index {index}. Closing open BUY position...")
            #     close_order = app.create_order(app.nextValidOrderId, "SELL", "MKT", 100, outsideRth=outside_rth)
            #     app.placeOrder(app.nextValidOrderId, contract, close_order)
            #     self.in_long_position = False
            #     self.alert_active = False
            #     # print(
            #     #     f"[DEBUG] Long position closed - in_long_position: {self.in_long_position}, alert_active: {self.alert_active}")
            #     if app.nextValidOrderId is not None:
            #         self.update_position_status(app.nextValidOrderId - 3, 'Closed')
            #     else:
            #         print("[ERROR] nextValidOrderId is None. Cannot update position status.")
            #     self.display_positions()
            #
            #     if app.profit_taker_order_id:
            #         app.cancel_open_order(app.profit_taker_order_id)
            #     if app.stop_loss_order_id:
            #         app.cancel_open_order(app.stop_loss_order_id)

            # elif action == 'exit_short':
            #     print(f"Short Exit detected at index {index}. Closing open SELL position...")
            #     close_order = app.create_order(app.nextValidOrderId, "BUY", "MKT", 100, outsideRth=outside_rth)
            #     app.placeOrder(app.nextValidOrderId, contract, close_order)
            #     self.in_short_position = False
            #     self.alert_active = False
            #     # print(
            #     #     f"[DEBUG] Short position closed - in_short_position: {self.in_short_position}, alert_active: {self.alert_active}")
            #     if app.nextValidOrderId is not None:
            #         self.update_position_status(app.nextValidOrderId - 3, 'Closed')
            #     else:
            #         print("[ERROR] nextValidOrderId is None. Cannot update position status.")
            #     self.display_positions()
            #
            #     if app.profit_taker_order_id:
            #         app.cancel_open_order(app.profit_taker_order_id)
            #     if app.stop_loss_order_id:
            #         app.cancel_open_order(app.stop_loss_order_id)

            stop_flag.set()
            # print("2. Continue current program")
            # print("3. Return to main menu")
            # choice = input("Enter 1, 2, or 3: ")

            # if choice == '1':
            #     decision_queue.put(('cancel', None, None))
            # elif choice == '2':
            #     decision_queue.put(('continue', None, None))
            # elif choice == '3':
            #     decision_queue.put(('menu', None, None))
            #     stop_flag.set()
            # else:
            #     print("Invalid choice. Please try again.")
            sleep(1)

    # def display_orders(self):
    #     orders = list(self.order_queue.queue)
    #     for order in orders:
    #         print(f"Order ID: {order.orderId}, Action: {order.action}, Quantity: {order.totalQuantity}")
    # ena antistoixo pou tha vlepw mono ta positions

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
                # self.display_orders()  # Εμφάνιση των τρεχουσών εντολών
                order_id = input("Enter the Order ID to cancel: ")
                try:
                    from ib_api import IBApi
                    from data_processing import DataProcessor
                    from database import Database

                    db = Database()
                    data_processor = DataProcessor(db)
                    app = IBApi(data_processor, db)

                    order_id = int(order_id)
                    app.cancel_open_order(order_id)
                    # print(f"Order {order_id} has been cancelled.")
                except ValueError:
                    print("Invalid Order ID. Please enter a numeric value.")
            elif choice == '2':
                decision_queue.put('continue', None, None)
            elif choice == '3':
                decision_queue.put('menu', None, None)
                stop_flag.set()
            else:
                print("Invalid choice. Please try again.")
