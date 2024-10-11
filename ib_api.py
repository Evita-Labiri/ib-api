import queue

import pandas as pd
import pytz
from time import sleep, time
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
# from globals import stop_flag
from order_manager import OrderManager
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("ib_api.log")]
)
logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]


class IBApi(EClient, EWrapper):
    def __init__(self, data_processor, db):
        EClient.__init__(self, wrapper=self)
        self.data_download_complete = False
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = {}
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
        logger.error(f"Error: {reqId} {errorCode} {errorString} {advancedOrderRejectJson}")
        # print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        logger.info(f"Next Valid Order ID: {orderId}")
        # print(f"Next Valid Order ID: {orderId}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        logger.info(f"Order Status: {orderId}, Status: {status}")
        print(f"Order Status: {orderId}, Status: {status}")
        if status in ['PreSubmitted', 'Submitted']:
            logger.info(f"Order {orderId} has been successfully placed with status: {status}")
            print(f"Order {orderId} has been successfully placed with status: {status}")
        elif status == 'Filled':
            logger.info(f"Order {orderId} has been filled.")
            print(f"Order {orderId} has been filled.")
        elif status in ['Cancelled', 'Inactive']:
            logger.error(f"Order {orderId} failed with status: {status}")
            print(f"Order {orderId} failed with status: {status}")
        else:
            logger.warning(f"Order {orderId} is in an unknown state: {status}")
            print(f"Order {orderId} is in an unknown state: {status}")

        self.order_manager.handle_order_execution(orderId, status)

    def openOrder(self, orderId, contract, order, orderState):
        print(
            f"Open Order - orderId: {orderId}, contract: {contract.symbol}, orderType: {order.orderType}, action: {order.action}, totalQuantity: {order.totalQuantity}")

    def execDetails(self, reqId, contract, execution):
        print(
            f"Exec Details - reqId: {reqId}, symbol: {contract.symbol}, execId: {execution.execId}, orderId: {execution.orderId}, shares: {execution.shares}, lastLiquidity: {execution.lastLiquidity}")

    def historicalData(self, reqId, bar):
        logger.debug("Requesting data...")
        logger.debug(
            f'Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}')
        print("Requesting data...")
        print(
            f'Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}')
        try:
            # contract = next((entry['contract'] for entry in self.contracts if entry['reqId'] == reqId), None)
            if reqId in self.reqId_info:
                contract_info = self.reqId_info[reqId]
                contract = contract_info['contract']
                data_type = contract_info['data_type']
                gr_tz = pytz.timezone('Europe/Athens')
                ny_tz = pytz.timezone('America/New_York')

                if data_type == 'minute':
                    date_parts = bar.date.split()

                    if len(date_parts) == 3:
                        date_str, time_str, tz_str = date_parts
                        date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
                        # print(f"Date with timezone: {date} (Timezone: {tz_str})")
                        date = gr_tz.localize(date)
                        date_ny = date.astimezone(ny_tz)

                    elif len(date_parts) == 2:
                        date_str, time_str = date_parts
                        date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
                        print(f"Date without timezone: {date}")
                        date = gr_tz.localize(date)
                        date_ny = date.astimezone(ny_tz)
                    self.db.insert_data_to_minute_table('minute_data', contract.symbol, date_ny, bar.open, bar.high, bar.low,
                                                    bar.close, bar.volume)

                elif data_type == 'daily':
                    date_str = bar.date
                    date = datetime.strptime(date_str, '%Y%m%d')
                    date = ny_tz.localize(date).replace(tzinfo=None)
                    self.db.insert_data_to_daily_table('daily_data', contract.symbol, date, bar.open, bar.high, bar.low,
                                                       bar.close, bar.volume)
            else:
                logger.error(f"No contract found for reqId: {reqId}")
                print(f"No contract found for reqId: {reqId}")

        except ValueError as e:
            logger.error(f"Error converting date: {e}")
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
        logger.info("Historical data download complete")
        print("Historical data download complete")
        self.data_download_complete = True
        self.data_processor.data_ready_queue.put(self.data)

    def get_reqId_for_contract(self, contract):
        for reqId, info in self.reqId_info.items():
            if info['contract'] == contract:
                logger.info(f"Found reqId {reqId} for contract {contract.symbol}")
                # print(f"Found reqId {reqId} for contract {contract.symbol}")
                return reqId
        logger.warning(f"No reqId found for contract: {contract.symbol}")
        # print(f"No reqId found for contract {contract.symbol}")
        return None

    def update_minute_data_for_symbol(self, contract):
        reqId = self.get_reqId_for_contract(contract)
        # print(f"Processing historical data for reqId: {reqId}...")

        if reqId is None:
            logger.warning(f"No reqId found for contract: {contract.symbol}")
            # print(f"No reqId found for contract: {contract.symbol}")
            return

        contract_info = self.reqId_info[reqId]
        data_type = contract_info['data_type']
        logger.info(f"Contract found: {contract.symbol}, data_type: {data_type}")
        # print(f"Contract found: {contract.symbol}, data_type: {data_type}")

        last_date = self.db.get_last_date_for_symbol(contract.symbol)
        if last_date is None:
            start_date = "20240826 09:13:00"
        else:
            ny_tz = pytz.timezone('America/New_York')
            start_date = last_date.astimezone(ny_tz).strftime(
                '%Y%m%d %H:%M:%S')

        duration_str = "5 D"
        logger.info(f"Requesting historical data for {contract.symbol}, from {start_date}")
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

    def check_connection(self):
        if not self.isConnected():
            logger.warning("API disconnected, attempting to reconnect...")
            # print("API disconnected, attempting to reconnect...")
            self.connect("127.0.0.1", 7497, 1)
            if self.isConnected():
                logger.info("Reconnected to API.")
                # print("Reconnected to API.")
                sleep(2)
            else:
                logger.error("Failed to reconnect, setting stop_flag.")
                # print("Failed to reconnect, setting stop_flag.")
    #             stop_flag.set()

    def tickPrice(self, reqId, tickType, price, attrib):
        # print(f"Tick Price for reqId {reqId}: {price}")
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        # print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')
        # logger.info(f'Tick Price. reqId: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')

        if tickType == 4:  # Last
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

                contract_info = self.reqId_info.get(reqId)
                if contract_info:
                    contract = contract_info['contract']
                    ticker = contract.symbol
                    logger.info(f"Found contract {ticker} for reqId {reqId}")
                    # print(f"Found contract {ticker} for reqId {reqId}")

                    # Αποθήκευση των δεδομένων ως DataFrame
                    if ticker not in self.real_time_data:
                        self.real_time_data[ticker] = pd.DataFrame(
                            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

                    new_row = pd.DataFrame([{
                        'Date': timestamp,
                        'Open': self.ohlcv_data[reqId]['open'],
                        'High': self.ohlcv_data[reqId]['high'],
                        'Low': self.ohlcv_data[reqId]['low'],
                        'Close': self.ohlcv_data[reqId]['close'],
                        'Volume': float(self.ohlcv_data[reqId]['volume']),
                        'Ticker': ticker
                    }])

                    self.real_time_data[ticker] = pd.concat([self.real_time_data[ticker], new_row], ignore_index=True)
                    self.data_processor.data_ready_queue.put(self.real_time_data[ticker].iloc[-1])
                    # self.data_processor.update_plot(contract=contract,
                    #                                 interval_entry=self.data_processor.interval_entry,
                    #                                 interval_exit=self.data_processor.interval_exit)


                else:
                    logger.warning(f"Contract not found for reqId: {reqId}")
                    # print(f"Contract not found for reqId: {reqId}")

    def tickSize(self, reqId, tickType, size):
        # print(f"Tick Size for reqId {reqId}: {size}")
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        # print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}, Timestamp: {timestamp}')
        # logger.info(f'Tick Size. reqId: {reqId}, tickType: {tickType}, Size: {size}, Timestamp: {timestamp}')

        contract_info = self.reqId_info.get(reqId)
        if contract_info:
            contract = contract_info['contract']
            ticker = contract.symbol
            logger.info(f"Found contract {ticker} for reqId {reqId}")
            # print(f"Found contract {ticker} for reqId {reqId}")

            if tickType == 5:  # Volume
                with self.lock:
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

                    if ticker not in self.real_time_data:
                        self.real_time_data[ticker] = pd.DataFrame(
                            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

                    new_row = pd.DataFrame([{
                        'Date': timestamp,
                        'Open': self.ohlcv_data[reqId]['open'],
                        'High': self.ohlcv_data[reqId]['high'],
                        'Low': self.ohlcv_data[reqId]['low'],
                        'Close': self.ohlcv_data[reqId]['close'],
                        'Volume': float(self.ohlcv_data[reqId]['volume']),
                        'Ticker': ticker
                    }])

                    self.real_time_data[ticker] = pd.concat([self.real_time_data[ticker], new_row], ignore_index=True)
                    self.data_processor.data_ready_queue.put(self.real_time_data[ticker].iloc[-1])
                    logger.info(f"Appended real-time data size for {ticker}: {self.real_time_data[ticker].iloc[-1]}")
                    # print(f"Appended real-time data size for {ticker}: {self.real_time_data[ticker].iloc[-1]}")
                    # self.data_processor.update_plot(contract=contract,
                    #                                 interval_entry=self.data_processor.interval_entry,
                    #                                 interval_exit=self.data_processor.interval_exit)
        else:
            logger.warning(f"Contract not found for reqId: {reqId}")
            # print(f"Contract not found for reqId: {reqId}")

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
        order.tif = "DAY"
        logger.info(f"Created order: {order}")
        # print(f"Created order: {order}")
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

        logger.info(f"Parent Order ID: {parentOrderId}, Action: {action}, Limit Price: {limit_price}")
        logger.info(
            f"Take Profit Order ID: {parentOrderId + 1}, Action: {take_profit_action}, Limit Price: {profit_target}")
        logger.info(f"Stop Loss Order ID: {parentOrderId + 2}, Action: {stop_loss_action}, Stop Price: {stop_loss}")

        # print(f"Parent Order ID: {parentOrderId}, Action: {action}, Limit Price: {limit_price}")
        # print(f"Take Profit Order ID: {parentOrderId + 1}, Action: {take_profit_action}, Limit Price: {profit_target}")
        # print(f"Stop Loss Order ID: {parentOrderId + 2}, Action: {stop_loss_action}, Stop Price: {stop_loss}")

        return [parent, take_profit, stop_loss]

    def place_bracket_order(self, contract, action, quantity, limit_price, profit_target, stop_loss, outside_rth=False):
        order_lock = threading.Lock()
        with order_lock:

            try:
                # retries = 3  # Μέγιστος αριθμός προσπαθειών
                # delay = 2  # Χρόνος αναμονής (δευτερόλεπτα) μεταξύ προσπαθειών

                bracket = self.create_bracket_order(self.nextValidOrderId, action, quantity, limit_price, profit_target,
                                                        stop_loss, outside_rth)

                # Store the order IDs
                self.entry_order_id = bracket[0].orderId
                self.profit_taker_order_id = bracket[1].orderId
                self.stop_loss_order_id = bracket[2].orderId

                for o in bracket:
                    if not self.check_connection():
                        logger.warning("API disconnected before placing order. Attempting to reconnect.")
                        # print("API disconnected before placing order. Attempting to reconnect.")
                    self.placeOrder(o.orderId, contract, o)
                    logger.info(f"Placed order: {o.orderId} for contract: {contract.symbol}")
                    # print(f"Placed order: {o.orderId} for contract: {contract.symbol}")
                    self.nextValidOrderId += 1
                    sleep(1)
                return bracket
            except Exception as e:
                logger.error(f"Error placing bracket order: {str(e)}")
                # print(f"Error placing bracket order: {str(e)}")
                return None

    def cancel_open_order(self, order_id):
        logger.info(f"Cancelling order ID: {order_id}")
        print(f"Cancelling order ID: {order_id}")
        # manual_cancel_order_time = datetime.now().strftime('%Y%m%d %H:%M:%S')
        # self.cancelOrder(order_id, manualCancelOrderTime=manual_cancel_order_time)
        # OrderManager.in_long_position = False
        # OrderManager.in_short_position = False
        # OrderManager.alert_active = False

    # def cancel_open_order(self, order_id):
    #     if order_id is None:
    #         print(f"Cannot cancel order: Invalid order ID")
    #         return
    #
    #     print(f"Cancelling order ID: {order_id}")

        try:
            manual_cancel_order_time = datetime.now().strftime('%Y%m%d %H:%M:%S')
            self.cancelOrder(order_id, manualCancelOrderTime=manual_cancel_order_time)
            logger.info(f"Order {order_id} cancelled successfully.")
            print(f"Order {order_id} cancelled successfully.")

            if order_id == self.entry_order_id:
                OrderManager.in_long_position = False
                OrderManager.in_short_position = False
                OrderManager.alert_active = False
                logger.info(f"Flags updated after cancelling order {order_id}")
                # print(f"Flags updated after cancelling order {order_id}")

        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
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

        self.reqId_info[reqId] = {
            'contract': contract,
            'data_type': data_type
        }

        logger.info(
            f"Created contract for symbol: {symbol}, secType: {sec_type}, exchange: {exchange}, currency: {currency}")
        logger.info(f"Assigned reqId: {reqId} for contract with symbol: {symbol}")

        return contract
    # def reset_reqId(self, start_value=1):
    #     self.current_reqId = start_value

    def main_thread_function(self, interval_entry, interval_exit):
        while True:
            if not self.data_processor.data_ready_queue.empty():
                _ = self.data_processor.data_ready_queue.get()

                for contract_dict in self.contracts:
                    contract = contract_dict['contract']
                    if contract:
                        self.data_processor.update_plot(interval_entry=interval_entry,interval_exit=interval_exit, contract=contract)
                    else:
                        logger.warning(f"Contract for reqId {contract_dict['reqId']} is not set.")
                        # print(f"Contract for reqId {contract_dict['reqId']} is not set.")

    def order_main_thread_function(self, data_processor, interval_entry, interval_exit, contracts, order_manager, decision_queue,
                               decision_flag):
        logger.info(f"Received contracts: {contracts}")

        # print(f"Received contracts: {contracts}")
        while True:
            logger.debug("Running order main thread function")
            # print("Running order main thread function")
            combined_data_dict = {}
            for contract_dict in contracts:
                contract = contract_dict['contract']
                if contract:
                    symbol = contract.symbol
                    df_entry, df_exit = data_processor.update_plot(interval_entry=interval_entry,
                                                                   interval_exit=interval_exit, contract=contract)
                    # if df_entry.empty or df_exit.empty:
                    #     logger.warning(f"Entry or Exit data for {symbol} is empty.")
                    #     # print(f"Warning: Entry or Exit data for {symbol} is empty.")
                    # else:
                    #     logger.info(f"Data for {symbol} looks valid with {len(df_entry)} entry records and {len(df_exit)} exit records.")
                    #     # print(
                    #     #     f"Data for {symbol} looks valid with {len(df_entry)} entry records and {len(df_exit)} exit records.")
                    combined_data_dict[symbol] = {'entry': df_entry, 'exit': df_exit}
                else:
                    print(f"Contract for reqId {contract_dict['reqId']} is not set.")

            if combined_data_dict:
                print("Checking comb data dict")
                # order_manager.wait_for_market_time()
                # print(f"Combined data dictionary: {combined_data_dict}")
                for symbol, data in combined_data_dict.items():
                    if isinstance(data['entry'], pd.DataFrame) and isinstance(data['exit'], pd.DataFrame):
                        if not data['entry'].empty and not data['exit'].empty:
                            logger.info(f"Processing signals for {symbol}. Entry has {len(data['entry'])} rows and Exit has {len(data['exit'])} rows.")
                            # print(
                            #     f"Processing signals for {symbol}. Entry has {len(data['entry'])} rows and Exit has {len(data['exit'])} rows.")
                            order_manager.process_signals_and_place_orders(data['entry'], data['exit'], decision_queue,
                                                                           decision_flag, symbol)
                        else:
                            logger.warning(f"Warning: Entry or Exit data for {symbol} is empty.")
                            # print(f"Warning: Entry or Exit data for {symbol} is empty.")
                    else:
                        logger.error(f"Error: Invalid data type for {symbol}. Entry: {type(data['entry'])}, Exit: {type(data['exit'])}")
                        # print(
                        #     f"Error: Invalid data type for {symbol}. Entry: {type(data['entry'])}, Exit: {type(data['exit'])}")                else:
                logger.warning("No data to process signals.")
                # print("No data to process signals.")

            # try:
            #     # decision_queue.get(timeout=1)
            #     logger.info("Decision queue is not empty, handling decision")
            #     print("Decision queue is not empty, handling decision")
            # except queue.Empty:
            #     logger.warning("Decision queue is empty, no signals to process")
            #     print("Decision queue is empty, no signals to process")

    def close_connection(self):
        logger.info("Closing connection to IB API")
        self.disconnect() #Closes conn with IB API
        self.db.db_close_connection()
