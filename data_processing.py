import queue
from datetime import datetime, timedelta
import pandas as pd

from order_manager import OrderManager
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("ib_api.log")]
)

logger_5min = logging.getLogger('logger_5min')
logger_5min.setLevel(logging.INFO)

file_handler_5min = logging.FileHandler('5min_data.log')
file_handler_5min.setLevel(logging.INFO)
formatter_5min = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler_5min.setFormatter(formatter_5min)
logger_5min.addHandler(file_handler_5min)

logger_1min = logging.getLogger('logger_1min')
logger_1min.setLevel(logging.INFO)

file_handler_1min = logging.FileHandler('1min_data.log')
file_handler_1min.setLevel(logging.INFO)
formatter_1min = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler_1min.setFormatter(formatter_1min)
logger_1min.addHandler(file_handler_1min)

logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]


class DataProcessor:
    def __init__(self, db, api_helper):
        self.db = db
        self.api_helper = api_helper
        self.real_time_data = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])
        self.interval = None
        self.data_ready_queue = queue.Queue()
        self.data_in_long_position = False
        self.data_in_short_position = False
        self.place_orders_outside_rth = False
        self.order_manager = OrderManager(api_helper)
        self.export_buffer = {}
        # self.cached_df = None

        # self.excel_lock = threading.Lock()

    def process_queue_data(self):
        while not self.data_ready_queue.empty():
            data = self.data_ready_queue.get()
            # logger.info(f"Data fetched from queue: {data}")
            # print(f"Data fetched from queue: {data}")

            if isinstance(data, pd.Series):
                data = data.to_dict()
            if isinstance(data, dict):
                ticker = data.get('Ticker')
                if not ticker:
                    logger.warning("Ticker missing in data, skipping...")
                    continue

                required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
                missing_columns = [col for col in required_columns if col not in data]
                if missing_columns:
                    logger.warning(f"Missing columns in data for {ticker}: {missing_columns}")
                    continue

                new_row = pd.DataFrame([data], columns=required_columns)
                self.real_time_data = pd.concat([self.real_time_data, new_row], ignore_index=True)
                # logger.debug(f"Updated DataFrame with new data: {self.real_time_data.tail()}")
        return self.real_time_data

    def fetch_data_from_db(self, table_name, start_date=None, end_date=None, ticker=None):
        return self.db.fetch_data_from_db(table_name, start_date, end_date, ticker)

    @staticmethod
    def calculate_indicators(df):
        if len(df) < 200:
            logger.warning("Not enough data to calculate indicators")
            print("Not enough data to calculate indicators")
            return df

        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()

        df['Session'] = (df['Date'].dt.date != df['Date'].shift(1).dt.date).astype(int).cumsum()
        df['Typical_Price'] = (df['Close'] + df['High'] + df['Low']) / 3
        df['Cumulative_Typical_Price_Volume'] = (df['Typical_Price'] * df['Volume']).groupby(df['Session']).cumsum()
        df['Cumulative_Volume'] = df['Volume'].groupby(df['Session']).cumsum()
        df['VWAP'] = df['Cumulative_Typical_Price_Volume'] / df['Cumulative_Volume']
        df['VWAP'] = df['VWAP'].fillna(0)

        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['BB_Middle'] = df['SMA20']
        df['BB_Upper'] = df['SMA20'] + 2 * df['Close'].rolling(window=20).std()
        df['BB_Lower'] = df['SMA20'] - 2 * df['Close'].rolling(window=20).std()

        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

        columns_to_remove = ['SMA20', 'Typical_Price', 'Cumulative_Typical_Price_Volume', 'Cumulative_Volume']
        df = df.drop(columns=[col for col in columns_to_remove if col in df])

        logger.info("Indicators calculated successfully.")
        return df

    def generate_signals(self, df):
        required_columns = ['EMA9', 'EMA20', 'EMA200', 'VWAP', 'MACD', 'MACD_Signal', 'BB_Upper', 'BB_Lower']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Not enough data to calculate {col}")
                print(f"Not enough data to calculate {col}")
                return df

        # Initialize the signals columns
        entry_long_criteria = [
            'EMA9_above_EMA20_long', 'EMA9_and_EMA20_above_EMA200_long',
            'Close_above_VWAP_long', 'MACD_above_Signal_long'
        # , 'MACD_above_zero_long',
        ]
        # 'Price_crossed_above_BB_Lower_long'

        entry_short_criteria = [
            'EMA9_below_EMA20_short', 'EMA9_and_EMA20_below_EMA200_short',
            'Close_below_VWAP_short', 'MACD_below_Signal_short'

        #, 'MACD_below_zero_short', 'Price_crossed_below_BB_Upper_short'
        ]

        exit_long_criteria = [
            'EMA9_below_EMA20_exit_long', 'Close_below_VWAP_exit_long', 'MACD_below_Signal_exit_long',
        ]
        # 'Price_touches_BB_Upper_exit_long'

        exit_short_criteria = [
            'EMA9_above_EMA20_exit_short', 'Close_above_VWAP_exit_short', 'MACD_above_Signal_exit_short',

        ]
        # 'Price_touches_BB_Lower_exit_short'

        # Initialize the signal columns with False
        for col in entry_long_criteria + entry_short_criteria + exit_long_criteria + exit_short_criteria:
            df[col] = False

        df['Long_Entry'] = False
        df['Short_Entry'] = False
        df['Long_Exit'] = False
        df['Short_Exit'] = False

        for i in range(2, len(df)):
            try:
                if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(
                        df['EMA20'].iloc[i]) or pd.isna(df['EMA200'].iloc[i]):
                    continue

                # print(f"Date: {df['Date'].iloc[i]}")
                # print(f"EMA9[i-1]: {df['EMA9'].iloc[i - 1]}, EMA9[i-2]: {df['EMA9'].iloc[i - 2]}")
                # print(f"EMA20[i-1]: {df['EMA20'].iloc[i - 1]}, EMA20[i-2]: {df['EMA20'].iloc[i - 2]}")
                # print(f"EMA200[i-1]: {df['EMA200'].iloc[i - 1]}, EMA200[i-2]: {df['EMA200'].iloc[i - 2]}")
                # print(f"Close[i-1]: {df['Close'].iloc[i - 1]}, VWAP[i-1]: {df['VWAP'].iloc[i - 1]}")
                # print(f"MACD[i-1]: {df['MACD'].iloc[i - 1]}, MACD_Signal[i-2]: {df['MACD_Signal'].iloc[i - 2]}")

                # Long Entry Criteria
                if df['EMA9'].iloc[i - 1] > df['EMA20'].iloc[i - 1] and df['EMA9'].iloc[i - 1] > df['EMA20'].iloc[i - 2]:
                    df.at[i, 'EMA9_above_EMA20_long'] = True
                if df['EMA9'].iloc[i - 1] > df['EMA200'].iloc[i - 1] and df['EMA9'].iloc[i - 1] > df['EMA200'].iloc[i - 2]:
                    if df['EMA20'].iloc[i - 1] > df['EMA200'].iloc[i - 1] and df['EMA20'].iloc[i - 1] > df['EMA200'].iloc[i - 2]:
                        df.at[i, 'EMA9_and_EMA20_above_EMA200_long'] = True
                # if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
                #     df.at[i, 'EMA20_above_EMA200_long'] = True
                if df['Close'].iloc[i - 1] > df['VWAP'].iloc[i - 1] and df['Close'].iloc[i - 1] > df['VWAP'].iloc[i - 2]:
                    df.at[i, 'Close_above_VWAP_long'] = True
                if df['MACD'].iloc[i - 1] > df['MACD_Signal'].iloc[i - 1] and df['MACD'].iloc[i - 1] > df['MACD_Signal'].iloc[i - 2]:
                    df.at[i, 'MACD_above_Signal_long'] = True
                # tha prepei na isxysoyn kai ta 4 prohgoymena gia na mpoyme se order
                # if df['MACD'].iloc[i - 1] > 0 and df['MACD'].iloc[i - 2] > 0:
                #     df.at[i, 'MACD_above_zero_long'] = True
                # if df['Close'].iloc[i - 1] < df['BB_Lower'].iloc[i - 1] and df['Close'].iloc[i - 2] >= df['BB_Lower'].iloc[
                #     i - 2]:
                #     df.at[i, 'Price_crossed_above_BB_Lower_long'] = True
                #
                # if any(df.loc[i, entry_long_criteria]):
                #     df.at[i, 'Long_Entry'] = True
                #     self.data_in_long_position = True
                #     self.order_manager.open_long_position()

                first_four_criteria_long = (
                        df.at[i, 'EMA9_above_EMA20_long'] and
                        df.at[i, 'EMA9_and_EMA20_above_EMA200_long'] and
                        df.at[i, 'Close_above_VWAP_long'] and
                        df.at[i, 'MACD_above_Signal_long']
                )

                if first_four_criteria_long :   #or df['MACD_above_zero_long'].iloc[i] == True
                    df.at[i, 'Long_Entry'] = True
                    self.data_in_long_position = True
                    self.order_manager.open_long_position()
                #
                # # For testing reasons
                # if any(df.loc[i, entry_long_criteria]):
                #     df.at[i, 'Long_Entry'] = True
                #     self.order_manager.close_long_position()

                # Short Entry Criteria
                if df['EMA9'].iloc[i - 1] < df['EMA20'].iloc[i - 1] and df['EMA9'].iloc[i - 1] < df['EMA20'].iloc[i - 2]:
                    df.at[i, 'EMA9_below_EMA20_short'] = True
                if df['EMA9'].iloc[i - 1] < df['EMA200'].iloc[i - 1] and df['EMA9'].iloc[i - 1] < df['EMA200'].iloc[i - 2]:
                    if df['EMA20'].iloc[i - 1] < df['EMA200'].iloc[i - 1] and df['EMA20'].iloc[i - 1] < df['EMA200'].iloc[i - 2]:
                        df.at[i, 'EMA9_and_EMA20_below_EMA200_short'] = True
                # if df['EMA20'].iloc[i - 1] < df['EMA200'].iloc[i - 1]:
                #     df.at[i, 'EMA20_below_EMA200_short'] = True
                if df['Close'].iloc[i - 1] < df['VWAP'].iloc[i - 1] and df['Close'].iloc[i - 1] < df['VWAP'].iloc[i - 2]:
                    df.at[i, 'Close_below_VWAP_short'] = True
                if df['MACD'].iloc[i - 1] < df['MACD_Signal'].iloc[i - 1] and df['MACD'].iloc[i - 1] < df['MACD_Signal'].iloc[i - 2]:
                    df.at[i, 'MACD_below_Signal_short'] = True
                # if df['MACD'].iloc[i - 1] < 0 and df['MACD'].iloc[i - 2] < 0:
                #     df.at[i, 'MACD_below_zero_short'] = True
                # if df['Close'].iloc[i - 1] > df['BB_Upper'].iloc[i - 1] and df['Close'].iloc[i - 2] <= df['BB_Upper'].iloc[
                #     i - 2]:
                #     df.at[i, 'Price_crossed_below_BB_Upper_short'] = True

                first_four_criteria_short = (
                        df.at[i, 'EMA9_below_EMA20_short'] and
                        df.at[i, 'EMA9_and_EMA20_below_EMA200_short'] and
                        df.at[i, 'Close_below_VWAP_short'] and
                        df.at[i, 'MACD_below_Signal_short']
                )

                if first_four_criteria_short:   # or df['MACD_below_zero_short'].iloc[i] == True
                    df.at[i, 'Short_Entry'] = True
                    self.data_in_short_position = True
                    self.order_manager.open_short_position()

                # # For testing reasons
                # if any(df.loc[i, entry_short_criteria]):
                #     df.at[i, 'Short_Entry'] = True
                #     self.order_manager.close_short_position()

                # Long Exit Criteria
                if self.data_in_long_position:
                    if df['EMA9'].iloc[i - 1] < df['EMA20'].iloc[i - 1] and df['EMA9'].iloc[i - 2] > df['EMA20'].iloc[i - 2]:
                        df.at[i, 'EMA9_below_EMA20_exit_long'] = True
                    if df['Close'].iloc[i- 1] < df['VWAP'].iloc[i - 1] and df['Close'].iloc[i - 2] > df['VWAP'].iloc[i - 2]:
                        df.at[i, 'Close_below_VWAP_exit_long'] = True
                    if df['MACD'].iloc[i - 1] < df['MACD_Signal'].iloc[i - 1] and df['MACD'].iloc[i - 2] > df['MACD_Signal'].iloc[i - 2]:
                        df.at[i, 'MACD_below_Signal_exit_long'] = True
                    # if df['Close'].iloc[i- 1] >= df['BB_Upper'].iloc[i- 1]:
                    #     df.at[i, 'Price_touches_BB_Upper_exit_long'] = True

                    if any(df.loc[i, exit_long_criteria]):
                        df.at[i, 'Long_Exit'] = True
                        self.order_manager.close_long_position()

                # Short Exit Criteria
                if self.data_in_short_position:
                    if df['EMA9'].iloc[i - 1] > df['EMA20'].iloc[i - 1] and df['EMA9'].iloc[i - 2] < df['EMA20'].iloc[i - 2]:
                        df.at[i, 'EMA9_above_EMA20_exit_short'] = True
                    if df['Close'].iloc[i - 1] > df['VWAP'].iloc[i - 1] and df['Close'].iloc[i - 2] < df['VWAP'].iloc[i - 2]:
                        df.at[i, 'Close_above_VWAP_exit_short'] = True
                    if df['MACD'].iloc[i - 1] > df['MACD_Signal'].iloc[i - 1] and df['MACD'].iloc[i - 2] < df['MACD_Signal'].iloc[i - 2]:
                        df.at[i, 'MACD_above_Signal_exit_short'] = True
                    # if df['Close'].iloc[i- 1] <= df['BB_Lower'].iloc[i- 1]:
                    #     df.at[i, 'Price_touches_BB_Lower_exit_short'] = True

                    if any(df.loc[i, exit_short_criteria]):
                        df.at[i, 'Short_Exit'] = True
                        self.order_manager.close_short_position()

            except KeyError as e:
                logger.error(f"KeyError at index {i}: {e}, skipping this index.")
                # print(f"KeyError at index {i}: {e}, skipping this index.")
                continue

            except Exception as e:
                logger.error(f"Unexpected error at index {i}: {e}, skipping this index.")
                # print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue

        logger.info("Signal generation completed.")
        return df

    def validate_interval(self, user_input):
        valid_intervals = ['1min', '2min', '3min', '4min', '5min', '10min', '15min', '30min', '1H']
        if user_input in valid_intervals:
            return user_input
        elif not user_input:  # if user_input is empty or None
            return None
        else:
            logger.warning(f"Invalid interval '{user_input}', using no interval.")
            # print(f"Invalid interval '{user_input}', using no interval.")
            return None

    @staticmethod
    def resample_data(df, interval):
        resampled_df = df.resample(interval).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'Ticker': 'first'
        }).ffill().reset_index()
        logger.info("Resampling Complete")
        return resampled_df

    def update_plot(self, contract, days=7,  interval_entry=None, interval_exit=None):
        # if self.cached_df.empty:
        #     end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        #
        #     df_minute = self.fetch_data_from_db('minute_data', start_date, end_date, ticker=contract.symbol)
        #     df_minute.set_index('Date', inplace=True)
        #     self.cached_df = df_minute
        #
        #     # Ενημέρωση cache με νέα δεδομένα σε πραγματικό χρόνο
        # real_time_data = self.process_queue_data()
        # real_time_df = pd.DataFrame(real_time_data,
        #                             columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])
        # real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')
        # real_time_df.set_index('Date', inplace=True)
        #
        # real_time_df_filtered = real_time_df[real_time_df['Ticker'] == contract.symbol]
        #
        # # Προσθήκη των νέων δεδομένων στο cached DataFrame
        # combined_data = pd.concat([self.cached_df, real_time_df_filtered])
        #
        # # Αφαιρούμε παλαιότερα δεδομένα (κρατάμε μόνο τα τελευταία 200+ κεριά για τους δείκτες)
        # if len(combined_data) > 250:  # Μπορείς να το ρυθμίσεις ανάλογα με τους δείκτες που χρησιμοποιείς
        #     combined_data = combined_data.iloc[-250:]
        #
        # self.cached_df = combined_data  # Ενημέρωση cache
        real_time_data = self.process_queue_data()
        # logger.debug(f"Real-time Data List: {self.real_time_data}")
        # print("Real-time Data List:")
        # print(self.real_time_data)

        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date, ticker=contract.symbol)
        df_minute.set_index('Date', inplace=True)
        #
        # logger.info("Minute data from DB:")
        # logger.info(df_minute.tail())

        print("Minute data from DB:")
        print(df_minute.tail())
        #
        # print("Minute data DataFrame with datetime check:")
        # print(df_minute.dtypes)

        ticker = contract.symbol
        real_time_df = pd.DataFrame(real_time_data,
                                    columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])
        real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')

        # logger.debug(f"Real-time DataFrame for {ticker}:")
        # logger.debug(real_time_df)

        # print("Real time df: ")
        # print(real_time_df)

        # print("Real-time DataFrame with datetime check:")
        # print(real_time_df.dtypes)

        # print("Real-time DataFrame")
        # print(real_time_df)

        if 'Date' in real_time_df.columns:
            real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')
            real_time_df.set_index('Date', inplace=True)
            logger.debug(f"Valid real-time data found, DataFrame created for {ticker}")
            # print("Valid real-time data found, DataFrame created:")
            # print(real_time_df)
        else:
            logger.error(f"Column 'Date' is missing for {ticker}, cannot set as index")
            logger.error(real_time_df.columns)
            # print("Column 'Date' is missing, cannot set as index")
            # print(real_time_df.columns)

        logger.info(f"Processing real-time data for ticker: {ticker}")
        # print("Valid real-time data found, DataFrame created:")
        # print(real_time_df)

        # Εκτύπωση των real-time δεδομένων για έλεγχο
        # print("Real-time DataFrame before combining:")
        # print(real_time_df.tail())

        # filtered with ticker
        # print("Filtered with ticker")
        real_time_df_filtered = real_time_df[real_time_df['Ticker'] == ticker]
        logger.debug(f"Filtered real-time data for {ticker}:")
        logger.debug(real_time_df_filtered.tail())
        # print(real_time_df_filtered.tail())

        dataframes_to_concat = [df for df in [df_minute, real_time_df_filtered] if not df.empty]
        combined_data = pd.concat(dataframes_to_concat)
        # print('Combined Data right after combination')
        # print(combined_data.tail())

        if not combined_data.empty:
            if 'Date' in combined_data.columns:
                combined_data['Date'] = pd.to_datetime(combined_data['Date'], errors='coerce')
                combined_data.set_index('Date', inplace=True)
                combined_data.sort_values(by='Date', inplace=True)
                logger.info(f"Combined Data for {ticker}:")
                # logger.info(combined_data.tail())
                # print('Combined Data')
                # print(combined_data.tail())

            # Resample and process for entry signals
            print("Df_entry resampling")
            df_entry = self.resample_data(combined_data, interval_entry)
            print(df_entry.tail())

            # print("Df_entry indicators")
            df_entry = self.calculate_indicators(df_entry)
            # logger.info("Entry Indicators")
            # logger.info("\n" + df_entry.tail(20).to_string())
            # print(df_entry.tail())

            # print("Df_entry signals")
            df_entry = self.generate_signals(df_entry)
            # print(df_entry.tail())
            logger_5min.info("5-minute Entry Signals")
            logger_5min.info("\n" + df_entry.tail(20).to_string())

            # Resample and process for exit signals
            print("Df_exit resampling")
            df_exit = self.resample_data(combined_data, interval_exit)
            print(df_exit.tail())

            # print("Df_exit indicators")
            df_exit = self.calculate_indicators(df_exit)
            # logger.info("Exit Indicators")
            # logger.info("\n" + df_exit.tail(20).to_string())
            # print("Df_exit signals")

            df_exit = self.generate_signals(df_exit)
            # print(df_exit.tail())
            logger_1min.info("1-minute Exit Signals")
            logger_1min.info("\n" + df_exit.tail(20).to_string())

            # print(f"Exit {interval_exit} Data with Signals:")
            # print(df_exit.tail())
            #
            # print(f"Entry {interval_entry} Data with Signals:")
            # print(df_entry.tail())
            # logger.info(f"Entry {interval_entry} Data for {contract.symbol} with Signals:")
            # logger.info("\n" + df_entry.iloc[:, :2].join(df_entry.iloc[:, -4:]).tail(10).to_string())
            #
            # logger.info(f"Exit {interval_exit} Data for {contract.symbol} with Signals:")
            # logger.info("\n" + df_exit.iloc[:, :2].join(df_exit.iloc[:, -4:]).tail(10).to_string())

            # # Εκτύπωση των 2 πρώτων και των 4 τελευταίων στηλών για το df_entry
            # print(
            #     f"Entry {interval_entry} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
            # print(df_entry.iloc[:, :2].join(df_entry.iloc[:, -4:]))
            #
            # # Εκτύπωση των 2 πρώτων και των 4 τελευταίων στηλών για το df_exit
            # print(f"Exit {interval_exit} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
            # print(df_exit.iloc[:, :2].join(df_exit.iloc[:, -4:]))

            # self.export_buffer[f"{contract.symbol}_entry"] = df_entry
            # self.export_buffer[f"{contract.symbol}_exit"] = df_exit

            return df_entry, df_exit

        else:
            logger.warning(f"No data to process for {ticker}.")
            print("No data to process.")
            return None

    # def export_to_excel(self, dict_of_dfs, filename="final_output.xlsx"):
    #     try:
    #         if os.path.exists(filename):
    #             workbook = openpyxl.load_workbook(filename)
    #         else:
    #             workbook = openpyxl.Workbook()
    #             workbook.remove(workbook.active)
    #
    #         for ticker, df in dict_of_dfs.items():
    #             sheet_name = ticker[:31] if len(ticker) > 31 else ticker
    #
    #             if sheet_name in workbook.sheetnames:
    #                 worksheet = workbook[sheet_name]
    #                 # starting_row = worksheet.max_row + 1
    #                 for row in dataframe_to_rows(df, index=False,
    #                                              header=False):
    #                     worksheet.append(row)
    #             else:
    #                 worksheet = workbook.create_sheet(title=sheet_name)
    #                 for row in dataframe_to_rows(df, index=False, header=True):
    #                     worksheet.append(row)
    #
    #             for column_cells in worksheet.columns:
    #                 length = max(len(str(cell.value)) for cell in column_cells)
    #                 worksheet.column_dimensions[column_cells[0].column_letter].width = length
    #
    #         workbook.save(filename)
    #         print(f"Data successfully exported to {filename}")
    #
    #     except Exception as e:
    #         print(f"Error writing to Excel: {str(e)}")
    #
    # def export_to_excel_thread(self):
    #     while True:
    #         if self.export_buffer:
    #             logger.info("Starting export to Excel")
    #             for symbol_key in self.export_buffer:
    #                 if "entry" in symbol_key:
    #                     self.export_to_excel({symbol_key: self.export_buffer[symbol_key]}, filename="5min_data.xlsx")
    #                     logger.info(f"{symbol_key} - 5-minute data export completed")
    #             for symbol_key in self.export_buffer:
    #                 if "exit" in symbol_key:
    #                     self.export_to_excel({symbol_key: self.export_buffer[symbol_key]}, filename="1min_data.xlsx")
    #                     logger.info(f"{symbol_key} - 1-minute data export completed")

            # sleep(300)






