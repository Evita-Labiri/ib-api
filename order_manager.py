from datetime import datetime, time
import pytz

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

    def process_signals_and_place_orders(self, df, contract):
        for i in range(len(df)):
            try:
                print(
                    f"Processing index {i}: Long_Entry={df.at[i, 'Long_Entry']}, Short_Entry={df.at[i, 'Short_Entry']}, Long_Exit={df.at[i, 'Long_Exit']}, Short_Exit={df.at[i, 'Short_Exit']}")

                if df.at[i, 'Long_Entry'] and not self.in_long_position:
                    limit_price = df.at[i, 'Close']
                    profit_target = limit_price * 1.02  # Προκαθορισμένο 2% κέρδος
                    stop_loss = limit_price * 0.98  # Προκαθορισμένη 2% απώλεια

                    user_limit_price = input(f"Long Entry detected at index {i}. Enter limit price (default: {limit_price}): ")
                    user_profit_target = input(f"Enter profit target price (default: {profit_target}): ")
                    user_stop_loss = input(f"Enter stop loss price (default: {stop_loss}): ")

                    limit_price = float(user_limit_price) if user_limit_price else limit_price
                    profit_target = float(user_profit_target) if user_profit_target else profit_target
                    stop_loss = float(user_stop_loss) if user_stop_loss else stop_loss

                    self.check_and_place_order(contract, "BUY", 100, limit_price, profit_target, stop_loss)
                    self.in_long_position = True
                    self.in_short_position = False
                    print(f"Placed BUY bracket order at index {i}")

                elif df.at[i, 'Short_Entry'] and not self.in_short_position:
                    limit_price = df.at[i, 'Close']
                    profit_target = limit_price * 0.98  # Προκαθορισμένο 2% κέρδος
                    stop_loss = limit_price * 1.02  # Προκαθορισμένη 2% απώλεια

                    user_limit_price = input(f"Short Entry detected at index {i}. Enter limit price (default: {limit_price}): ")
                    user_profit_target = input(f"Enter profit target price (default: {profit_target}): ")
                    user_stop_loss = input(f"Enter stop loss price (default: {stop_loss}): ")

                    limit_price = float(user_limit_price) if user_limit_price else limit_price
                    profit_target = float(user_profit_target) if user_profit_target else profit_target
                    stop_loss = float(user_stop_loss) if user_stop_loss else stop_loss

                    self.check_and_place_order(contract, "SELL", 100, limit_price, profit_target, stop_loss)
                    self.in_short_position = True
                    self.in_long_position = False
                    print(f"Placed SELL bracket order at index {i}")

                elif df.at[i, 'Long_Exit'] and self.in_long_position:
                    self.app.cancel_open_order(self.app.nextValidOrderId - 1)
                    self.in_long_position = False
                    print(f"Cancelled BUY order (closing long position) at index {i}")

                elif df.at[i, 'Short_Exit'] and self.in_short_position:
                    self.app.cancel_open_order(self.app.nextValidOrderId - 1)
                    self.in_short_position = False
                    print(f"Cancelled SELL order (closing short position) at index {i}")

            except KeyError as e:
                print(f"KeyError at index {i}: {e}, skipping this index.")
                continue
            except Exception as e:
                print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue