from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading

class TradingApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        self.nextValidOrderId = None

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson):
        print("Error: {} {} {} {}".format(reqId, errorCode, errorString, advancedOrderRejectJson))

    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId

app = TradingApp()
app.connect("127.0.0.1",7497, 1)

t1 = threading.Thread(target=app.run)
t1.start()

while (app.nextValidId == None):
    print("Waiting for TWS connection aknowledgment...")

print("connection established")

