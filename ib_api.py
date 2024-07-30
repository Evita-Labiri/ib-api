import pytz
import queue
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading


class IBApi(EClient, EWrapper):
    def __init__(self, data_processor, db):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None
        self.data = []
        self.real_time_data = []
        self.lock = threading.Lock()

        self.historical_data_downloaded = False  # Flag to check if historical data is downloaded
        self.ohlcv_data = {}
        self.data_processor = data_processor
        self.db = db

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        print(
            f"Order Status - orderId: {orderId}, status: {status}, filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}")

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
        try:
            ny_tz = pytz.timezone('America/New_York')
            date = None
            if reqId == 1:
                date_str, time_str, tz_str = bar.date.split()
                date = datetime.strptime(f'{date_str} {time_str}', '%Y%m%d %H:%M:%S')
                date = ny_tz.localize(date).replace(tzinfo=None)
            elif reqId == 2:
                date_str = bar.date
                date = datetime.strptime(date_str, '%Y%m%d')
                date = ny_tz.localize(date).replace(tzinfo=None)
            self.data.append([date, bar.open, bar.high, bar.low, bar.close, bar.volume])
        except ValueError as e:
            print(f"Error converting date: {e}")

    def historicalDataEnd(self, reqId, start, end):
        print("Historical data download complete")
        self.data_processor.data_ready_queue.put(self.data)

    def tickPrice(self, reqId, tickType, price, attrib):
        print("Tick Price called now: ")
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Tick Price. Ticker Id: {reqId}, tickType: {tickType}, Price: {price}, Timestamp: {timestamp}')
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
                print(f"Appended real-time data: {self.real_time_data[-1]}")  # Debug print
                self.data_processor.update_plot(interval=self.data_processor.interval)
    def tickSize(self, reqId, tickType, size):
        print("Tick Size called now: ")
        timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        print(f'Tick Size. Ticker Id: {reqId}, tickType: {tickType}, Size: {size}, Timestamp: {timestamp}')

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

            print(f"Updated OHLCV data for {reqId}: {self.ohlcv_data[reqId]}")  # Debug print
            self.data_processor.update_plot(interval=self.data_processor.interval)

    def create_order(self, orderId, action, orderType, quantity, limitPrice=None, auxPrice=None):
        order = Order()
        order.orderId = orderId
        order.action = action
        order.orderType = orderType
        order.totalQuantity = quantity
        order.transmit = False
        if limitPrice is not None:
            order.lmtPrice = limitPrice
        if auxPrice is not None:
            order.auxPrice = auxPrice
        return order

    def create_bracket_order(self, parentOrderId, action, quantity, limit_price, profit_target, stop_loss):
        if action == "BUY":
            take_profit_action = "SELL"
            stop_loss_action = "SELL"
        else:
            take_profit_action = "BUY"
            stop_loss_action = "BUY"

        parent = self.create_order(parentOrderId, action, "LMT", quantity, limitPrice=limit_price)
        parent.transmit = False

        take_profit = self.create_order(parentOrderId + 1, take_profit_action, "LMT", quantity,
                                        limitPrice=profit_target)
        take_profit.parentId = parentOrderId
        take_profit.transmit = False

        stop_loss = self.create_order(parentOrderId + 2, stop_loss_action, "STP", quantity, auxPrice=stop_loss)
        stop_loss.parentId = parentOrderId
        stop_loss.transmit = True

        return [parent, take_profit, stop_loss]

    def place_bracket_order(self, contract, action, quantity, limit_price, profit_target, stop_loss):
        bracket = self.create_bracket_order(self.nextValidOrderId, action, quantity, limit_price, profit_target,
                                            stop_loss)
        for o in bracket:
            self.placeOrder(o.orderId, contract, o)

    def create_contract(self, symbol, secType, exchange, currency):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def main_thread_function(self, interval):
        while True:
            if not self.data_processor.data_ready_queue.empty():
                _ = self.data_processor.data_ready_queue.get()
                self.data_processor.update_plot(interval=interval)

    def close_connection(self):
        self.disconnect() #Closes conn with IB API
        self.db.db_close_connection()