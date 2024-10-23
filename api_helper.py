import ib_api
# Import necessary classes
from ib_api import IBApi
from data_processing import DataProcessor
from database import Database
from order_manager import OrderManager

class ApiHelper:

    def __init__(self, api_client):
        self.api_client = api_client

    db = Database()
    data_processor = DataProcessor(db, api_helper=None)  # Το api_helper μπορεί να παραμείνει None εδώ
    app = IBApi(data_processor, db)
    # api_helper = ApiHelper(app)

    def update_data(self, db, contract):
        self.api_client.update_minute_data_for_symbol(db, contract)

