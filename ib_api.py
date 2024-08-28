import queue

import pandas as pd
import pytz
from time import sleep
from datetime import datetime, time, timedelta
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
from order_manager import OrderManager

class IBApi(EClient, EWrapper):
    def __init__(self, data_processor, db):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = []
        self.lock = threading.Lock()
        self.ticker = None
        self.historical_data_downloaded = False  # Flag to check if historical data is downloaded
        self.ohlcv_data = {}
        self.data_processor = data_processor
        self.db = db
        self.order_manager = OrderManager()
        self.profit_taker_order_id = None
        self.stop_loss_order_id = None
        self.entry_order_id = None
        self.nextValidOrderId = None
        self.contracts = []
        self.current_reqId = 1
        self.reqId_info = {}

    def set_ticker(self, ticker):
        self.ticker = ticker

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print(f"Next Valid Order ID: {orderId}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        print(
            f"Order Status - orderId: {orderId}, status: {status}, filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}")

        self.order_manager.handle_order_execution(orderId, status)

    def openOrder(self, orderId, contract, order, orderState):
        print(
            f"Open Order - orderId: {orderId}, contract: {contract.symbol}, orderType: {order.orderType}, action: {order.action}, totalQuantity: {order.totalQuantity}")

    def execDetails(self, reqId, contract, execution):
        print(
            f"Exec Details - reqId: {reqId}, symbol: {contract.symbol}, execId: {execution.execId}, orderId: {execution.orderId}, shares: {execution.shares}, lastLiquidity: {execution.lastLiquidity}")

    def historicalData(self, reqId, bar):
        print("Requesting data...")
        print(
            f'Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}')
        # try:
        #     ny_tz = pytz.timezone('America/New_York')
        #     date = None
        #     if reqId == 1:  # Minute data
        #         date_str, time_str, tz_str = bar.date.split()
        #         date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
        #         date = ny_tz.localize(date).replace(tzinfo=None)
        #     elif reqId == 2:  # Daily data
        #         date_str = bar.date
        #         date = datetime.strptime(date_str, '%Y%m%d')
        #         date = ny_tz.localize(date).replace(tzinfo=None)
        #     ticker = self.ticker
        #     self.data.append([date, bar.open, bar.high, bar.low, bar.close, bar.volume])
        #     if reqId == 1:
        #         self.db.insert_data_to_minute_table('minute_data', ticker, date, bar.open, bar.high, bar.low, bar.close,
        #                                          bar.volume)
        #     elif reqId == 2:
        #         self.db.insert_data_to_daily_table('daily_data', ticker, date, bar.open, bar.high, bar.low,
        #                                            bar.close, bar.volume)
        # except ValueError as e:
        #     print(f"Error converting date: {e}")

        try:
            # contract = next((entry['contract'] for entry in self.contracts if entry['reqId'] == reqId), None)
            if reqId in self.reqId_info:
                contract_info = self.reqId_info[reqId]
                contract = contract_info['contract']
                data_type = contract_info['data_type']
                # Process and insert data based on data_type

                ny_tz = pytz.timezone('America/New_York')
                # date = None

                # date_str, time_str, tz_str = bar.date.split()

                # data_type = contract['data_type']  # Χρησιμοποιούμε το custom attribute
                if data_type == 'minute':
                    date_str, time_str, tz_str = bar.date.split()
                    date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
                    date = ny_tz.localize(date).replace(tzinfo=None)
                    self.db.insert_data_to_minute_table('minute_data', contract.symbol, date, bar.open, bar.high,
                                                        bar.low, bar.close, bar.volume)
                elif data_type == 'daily':
                    date_str = bar.date
                    date = datetime.strptime(date_str, '%Y%m%d')
                    date = ny_tz.localize(date).replace(tzinfo=None)
                    self.db.insert_data_to_daily_table('daily_data', contract.symbol, date, bar.open, bar.high, bar.low,
                                                       bar.close, bar.volume)
            else:
                print(f"No contract found for reqId: {reqId}")

        except ValueError as e:
            print(f"Error converting date: {e}")

    def insert_minute_data(self, ticker, date, bar):
        self.db.insert_data_to_minute_table(
            'minute_data', ticker, date, bar.open, bar.high, bar.low, bar.close, bar.volume
        )

    def insert_daily_data(self, ticker, date, bar):
        self.db.insert_data_to_daily_table(
            'daily_data', ticker, date, bar.open, bar.high, bar.low, bar.close, bar.volume
        )

    def historicalDataEnd(self, reqId, start, end):
        print("Historical data download complete")
        self.data_processor.data_ready_queue.put(self.data)

    def get_reqId_for_contract(self, contract):
        for reqId, info in self.reqId_info.items():
            if info['contract'] == contract:
                print(f"Found reqId {reqId} for contract {contract.symbol}")
                return reqId
        print(f"No reqId found for contract {contract.symbol}")
        return None

    def update_minute_data_for_symbol(self, contract):
        reqId = self.get_reqId_for_contract(contract)
        # print(f"Processing historical data for reqId: {reqId}...")

        if reqId is None:
            print(f"No reqId found for contract: {contract.symbol}")
            return

        contract_info = self.reqId_info[reqId]
        data_type = contract_info['data_type']
        print(f"Contract found: {contract.symbol}, data_type: {data_type}")

        last_date = self.db.get_last_date_for_symbol(contract.symbol)
        # Αν δεν υπάρχει, ξεκινήστε κατέβασμα από την αρχή ή κάποια άλλη προεπιλεγμένη ημερομηνία
        if last_date is None:
            start_date = "20240826 09:13:00"  # Θέστε μία αρχική ημερομηνία (χωρίς ζώνη ώρας)
        else:
            ny_tz = pytz.timezone('America/New_York')
            start_date = last_date.astimezone(ny_tz).strftime(
                '%Y%m%d %H:%M:%S')  # Μετατροπή ημερομηνίας σε σωστή μορφή

        # Δεν χρειάζεται το end_date για συνεχή λήψη δεδομένων
        duration_str = "1 D"  # Καθορίζει τη διάρκεια που θέλουμε να κατεβάσουμε τα δεδομένα

        # Κατεβάστε δεδομένα από την τελευταία ημερομηνία και συνεχίστε τη λήψη δεδομένων σε πραγματικό χρόνο
        self.reqHistoricalData(
            reqId=reqId,  # Χρησιμοποιήστε ένα μοναδικό reqId για κάθε αίτημα
            contract=contract,
            endDateTime="",  # Αφήστε το κενό για συνεχή λήψη δεδομένων
            durationStr=duration_str,  # Μπορείτε να προσαρμόσετε τη διάρκεια ανάλογα με τις ανάγκες σας
            barSizeSetting="1 min",  # Διάστημα ενός λεπτού
            whatToShow="TRADES",  # Είδος δεδομένων
            useRTH=0,
            formatDate=1,
            keepUpToDate=False,  # Συνεχής λήψη δεδομένων σε πραγματικό χρόνο
            chartOptions=[]
        )
        # Βρείτε την τελευταία ημερομηνία που έχετε δεδομένα στη βάση
        # self.reqMktData(
        #     4,
        #     contract,
        #     "",
        #     False,
        #     False,
        #     []
        # )

        # Αποθηκεύστε τα νέα δεδομένα στη βάση
        # new_data = []
        #
        # while not self.data_processor.data_ready_queue.empty():
        #     new_data.append(self.data_processor.data_ready_queue.get())
        #
        # if new_data:
        #     df_new_data = pd.DataFrame(new_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        #     df_new_data['Ticker'] = contract.symbol
        #
        #     # Αποθηκεύστε τα νέα δεδομένα στη βάση
        #     for index, row in df_new_data.iterrows():
        #         db.insert_data_to_minute_table('minute_data', row['Ticker'], row['Date'], row['Open'], row['High'],
        #                                        row['Low'], row['Close'], row['Volume'])
        #
        # print("Minute data updated successfully.")
        # else:
        #     print("No new data to update.")

        # self.data_processor.update_plot(contract=contract, interval=self.data_processor.interval)

    def calculate_duration(self, start_date, end_date):
        # Αφαιρούμε τη ζώνη ώρας από το start_date και το end_date για να ταιριάζουν με τη μορφή
        start_date = start_date.split(' ')[0] + ' ' + start_date.split(' ')[1]  # Αφαιρεί τη ζώνη ώρας
        end_date = end_date.split(' ')[0] + ' ' + end_date.split(' ')[1]  # Αφαιρεί τη ζώνη ώρας

        # Προσαρμόστε τη μορφή για να ταιριάζει με την ημερομηνία που έρχεται από τη βάση
        start = datetime.strptime(start_date, '%Y%m%d %H:%M:%S')
        end = datetime.strptime(end_date, '%Y%m%d %H:%M:%S')
        duration = end - start

        if duration.days > 0:
            return f"{duration.days} D"
        elif duration.seconds > 3600:
            hours = duration.seconds // 3600
            return f"{hours} H"
        else:
            minutes = duration.seconds // 60
            return f"{minutes} M"
    def tickPrice(self, reqId, tickType, price, attrib):
        # print("Tick Price called now: ")
        # timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        # print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')
        if tickType == 4:  # Last
            timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
            with self.lock:
                if reqId not in self.ohlcv_data:
                    self.ohlcv_data[reqId] = {
                        'timestamp': timestamp,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': 0,
                    }
                else:
                    self.ohlcv_data[reqId]['close'] = price
                    if price > self.ohlcv_data[reqId]['high']:
                        self.ohlcv_data[reqId]['high'] = price
                    if price < self.ohlcv_data[reqId]['low']:
                        self.ohlcv_data[reqId]['low'] = price

                self.real_time_data.append([
                    timestamp,
                    self.ohlcv_data[reqId]['open'],
                    self.ohlcv_data[reqId]['high'],
                    self.ohlcv_data[reqId]['low'],
                    self.ohlcv_data[reqId]['close'],
                    float(self.ohlcv_data[reqId]['volume'])
                ])
                self.data_processor.data_ready_queue.put(self.real_time_data[-1])
                # print(f"Appended real-time data: {self.real_time_data[-1]}")  # Debug print
                # contract_entry = next((entry for entry in self.contracts if entry['reqId'] == reqId), None)

                contract_info = self.reqId_info.get(reqId)
                if contract_info:
                    contract = contract_info['contract']
                    print(f"Found contract {contract.symbol} for reqId {reqId}")
                    self.data_processor.update_plot(contract=contract, interval=self.data_processor.interval)
                else:
                    print(f"Contract not found for reqId: {reqId}")

    def tickSize(self, reqId, tickType, size):
        # print("Tick Size called now: ")
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        # print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}, Timestamp: {timestamp}')

        if tickType == 5:  # Volume
            with (self.lock):
                if reqId in self.ohlcv_data:
                    self.ohlcv_data[reqId]['volume'] = size
                else:
                    self.ohlcv_data[reqId] = {
                        'timestamp': timestamp,
                        'open': 0,
                        'high': 0,
                        'low': 0,
                        'close': 0,
                        'volume': size
                    }

            # print(f"Updated OHLCV data for {reqId}: {self.ohlcv_data[reqId]}")  # Debug print
            # contract_entry = next((entry for entry in self.contracts if entry['reqId'] == reqId), None)
            # if contract_entry:
            #     contract = contract_entry['contract']
            #     self.data_processor.update_plot(contract=contract, interval=self.data_processor.interval)
            # else:
            #     print(f"Contract not found for reqId: {reqId}")
            contract_info = self.reqId_info.get(reqId)
            if contract_info:
                contract = contract_info['contract']
                print(f"Found contract {contract.symbol} for reqId {reqId}")
                self.data_processor.update_plot(contract=contract, interval=self.data_processor.interval)
            else:
                print(f"Contract not found for reqId: {reqId}")

    def create_order(self, orderId, action, orderType, quantity, limitPrice=None, auxPrice=None, outsideRth=False):
        order = Order()
        order.orderId = orderId
        order.action = action
        order.orderType = orderType
        order.totalQuantity = quantity
        order.transmit = False
        order.outsideRth = outsideRth
        if limitPrice is not None:
            order.lmtPrice = limitPrice
        if auxPrice is not None:
            order.auxPrice = auxPrice
        print(f"Created order: {order}")
        return order

    def create_bracket_order(self, parentOrderId, action, quantity, limit_price, profit_target, stop_loss,
                             outsideRth=False):
        if action == "BUY":
            take_profit_action = "SELL"
            stop_loss_action = "SELL"
        else:
            take_profit_action = "BUY"
            stop_loss_action = "BUY"

        parent = self.create_order(parentOrderId, action, "LMT", quantity, limitPrice=limit_price,
                                   outsideRth=outsideRth)
        parent.transmit = False

        take_profit = self.create_order(parentOrderId + 1, take_profit_action, "LMT", quantity,
                                        limitPrice=profit_target, outsideRth=outsideRth)
        take_profit.parentId = parentOrderId
        take_profit.transmit = False

        stop_loss = self.create_order(parentOrderId + 2, stop_loss_action, "STP", quantity, auxPrice=stop_loss,
                                      outsideRth=outsideRth)
        stop_loss.parentId = parentOrderId
        stop_loss.transmit = True

        return [parent, take_profit, stop_loss]

    def place_bracket_order(self, contract, action, quantity, limit_price, profit_target, stop_loss, outside_rth=False):
        bracket = self.create_bracket_order(self.nextValidOrderId, action, quantity, limit_price, profit_target,
                                                stop_loss, outside_rth)

        # Store the order IDs
        self.entry_order_id = bracket[0].orderId
        self.profit_taker_order_id = bracket[1].orderId
        self.stop_loss_order_id = bracket[2].orderId

        for o in bracket:
            self.placeOrder(o.orderId, contract, o)
            print(f"Placed order: {o.orderId} for contract: {contract.symbol}")
            self.nextValidOrderId += 1

    def cancel_open_order(self, order_id):
        print(f"Cancelling order ID: {order_id}")
        manual_cancel_order_time = datetime.now().strftime('%Y%m%d %H:%M:%S')
        self.cancelOrder(order_id, manualCancelOrderTime=manual_cancel_order_time)
        OrderManager.in_long_position = False
        OrderManager.in_short_position = False
        OrderManager.alert_active = False

    # def cancel_open_order(self, order_id):
    #     if order_id is None:
    #         print(f"Cannot cancel order: Invalid order ID")
    #         return
    #
    #     print(f"Cancelling order ID: {order_id}")

        try:
            manual_cancel_order_time = datetime.now().strftime('%Y%m%d %H:%M:%S')
            self.cancelOrder(order_id, manualCancelOrderTime=manual_cancel_order_time)
            print(f"Order {order_id} cancelled successfully.")

            # update flags here if this order is tied to an open position
            if order_id == self.entry_order_id:
                OrderManager.in_long_position = False
                OrderManager.in_short_position = False
                OrderManager.alert_active = False
                print(f"Flags updated after cancelling order {order_id}")

        except Exception as e:
            print(f"Failed to cancel order {order_id}: {e}")

    def create_contract(self, symbol, sec_type, exchange, currency, data_type, reqId=None):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        if reqId is None:
            reqId = self.current_reqId
            self.current_reqId += 1

        # Αποθηκεύουμε το reqId και το data_type στο εξωτερικό λεξικό
        self.reqId_info[reqId] = {
            'contract': contract,
            'data_type': data_type
        }

        return contract
    # def reset_reqId(self, start_value=1):
    #     self.current_reqId = start_value

    def main_thread_function(self, interval):
        while True:
            if not self.data_processor.data_ready_queue.empty():
                _ = self.data_processor.data_ready_queue.get()

                for contract_dict in self.contracts:
                    contract = contract_dict['contract']  # Παίρνουμε το πραγματικό αντικείμενο Contract από το λεξικό
                    if contract:  # Βεβαιωθείτε ότι το contract δεν είναι None
                        self.data_processor.update_plot(interval=interval, contract=contract)
                    else:
                        print(f"Contract for reqId {contract_dict['reqId']} is not set.")

    def order_main_thread_function(self, data_processor, interval, contracts, order_manager, decision_queue,
                                   decision_flag):
        print(f"Received contracts: {contracts}")

        while True:
            print("Running order main thread function")
            sleep(1)
            combined_data_dict = {}

            for contract_dict in contracts:
                contract = contract_dict['contract']
                if contract:
                    symbol = contract.symbol
                    combined_data = data_processor.update_plot(interval=interval, contract=contract)
                    print(f"Combined data for {symbol}: {combined_data.head()}")

                    if combined_data.empty:
                        print(f"Warning: Combined data for {symbol} is empty.")
                    else:
                        print(f"Data for {symbol} looks valid with {len(combined_data)} records.")

                    combined_data_dict[symbol] = combined_data

                else:
                    print(f"Contract for reqId {contract_dict['reqId']} is not set.")

            if combined_data_dict:
                order_manager.process_signals_and_place_orders(combined_data_dict, decision_queue, decision_flag)
            else:
                print("No data to process signals.")

            try:
                signal = decision_queue.get(timeout=5)  # Αν δεν υπάρχει signal μέσα σε 5 δευτερόλεπτα, συνέχισε
                print("Decision queue is not empty, handling decision")
                # order_manager.handle_decision(self, decision_queue, stop_flag)  # Εδώ θα χειριστείτε το signal
            except queue.Empty:
                print("Decision queue is empty, no signals to process")

    def close_connection(self):
        self.disconnect() #Closes conn with IB API
        self.db.db_close_connection()
