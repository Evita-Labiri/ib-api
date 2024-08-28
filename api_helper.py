class ApiHelper:
    def __init__(self, api_client):
        self.api_client = api_client

    def update_data(self, db, contract):
        self.api_client.update_minute_data_for_symbol(db, contract)