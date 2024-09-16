import queue
from datetime import datetime, timedelta, time
import pandas as pd

from order_manager import OrderManager

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
        self.order_manager = OrderManager()
        # self.ticker = None

    # def process_queue_data(self):
    #     while not self.data_ready_queue.empty():
    #         data = self.data_ready_queue.get()
    #
    #         if isinstance(data, dict):
    #             ticker = data['Ticker']
    #             if ticker not in self.real_time_data:
    #                 self.real_time_data[ticker] = []
    #
    #             self.real_time_data[ticker].append(data)

    # def process_queue_data(self):
    #     while not self.data_ready_queue.empty():
    #         data = self.data_ready_queue.get()
    #
    #         if isinstance(data, dict):
    #             ticker = data['Ticker']
    #             if ticker not in self.real_time_data:
    #                 self.real_time_data[ticker] = pd.DataFrame(
    #                     columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    #
    #             # Δημιουργία νέας γραμμής ως DataFrame
    #             new_row = pd.DataFrame([data], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    #
    #             # Προσθήκη της νέας γραμμής στο DataFrame του ticker
    #             self.real_time_data[ticker] = pd.concat([self.real_time_data[ticker], new_row], ignore_index=True)

    def process_queue_data(self):
        while not self.data_ready_queue.empty():
            data = self.data_ready_queue.get()

            # Εκτύπωση για να δείτε τι επιστρέφει η ουρά
            print(f"Data fetched from queue: {data}")

            if isinstance(data, pd.Series):
                data = data.to_dict()

            if isinstance(data, dict):
                ticker = data.get('Ticker')
                if not ticker:
                    print("Ticker missing in data, skipping...")
                    continue

                # Βεβαιωθείτε ότι τα δεδομένα περιέχουν όλες τις απαραίτητες στήλες
                required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
                missing_columns = [col for col in required_columns if col not in data]
                if missing_columns:
                    print(f"Missing columns in data for {ticker}: {missing_columns}")
                    continue

                # Δημιουργία νέας γραμμής ως DataFrame
                new_row = pd.DataFrame([data], columns=required_columns)

                # Προσθήκη της νέας γραμμής στο ενιαίο DataFrame
                self.real_time_data = pd.concat([self.real_time_data, new_row], ignore_index=True)

                # Εκτύπωση του DataFrame μετά την προσθήκη
                print(f"Updated DataFrame with new data:")
                print(self.real_time_data.tail())

        return self.real_time_data

    def fetch_data_from_db(self, table_name, start_date=None, end_date=None, ticker=None):
        return self.db.fetch_data_from_db(table_name, start_date, end_date, ticker)

    @staticmethod
    def calculate_indicators(df):
        if len(df) < 200:
            print("Not enough data to calculate indicators")
            return df

        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()

        df['Session'] = (df['Date'].dt.date != df['Date'].shift(1).dt.date).cumsum()
        df['Typical_Price'] = (df['Close'] + df['High'] + df['Low']) / 3
        df['Cumulative_Typical_Price_Volume'] = (df['Typical_Price'] * df['Volume']).groupby(df['Session']).cumsum()
        df['Cumulative_Volume'] = df['Volume'].groupby(df['Session']).cumsum()
        df['VWAP'] = df['Cumulative_Typical_Price_Volume'] / df['Cumulative_Volume']

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

        return df

    def generate_signals(self, df):
        required_columns = ['EMA9', 'EMA20', 'EMA200', 'VWAP', 'MACD', 'MACD_Signal', 'BB_Upper', 'BB_Lower']
        for col in required_columns:
            if col not in df.columns:
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

                # if any(df.loc[i, entry_long_criteria]):
                #     df.at[i, 'Long_Entry'] = True
                #     self.data_in_long_position = True
                #     self.order_manager.open_long_position()

                # first_four_criteria_long = (
                #         df['EMA9_above_EMA20_long'].iloc[i] == True and
                #         df['EMA9_and_EMA20_above_EMA200_long'].iloc[i] == True and
                #         df['Close_above_VWAP_long'].iloc[i] == True and
                #         df['MACD_above_Signal_long'] == True
                # )

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

                # if any(df.loc[i, entry_short_criteria]):
                #     df.at[i, 'Short_Entry'] = True
                #     self.data_in_short_position = True
                #     self.order_manager.open_short_position()
                #
                # first_four_criteria_short = (
                #         df[].iloc[i] == True and
                #         df[].iloc[i] == True and
                #         df[].iloc[i] == True and
                #         df[] == True
                # )

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
                print(f"KeyError at index {i}: {e}, skipping this index.")
                continue

            except Exception as e:
                print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue

        return df

    def validate_interval(self, user_input):
        valid_intervals = ['1min', '2min', '3min', '4min', '5min', '10min', '15min', '30min', '1H']
        if user_input in valid_intervals:
            return user_input
        elif not user_input:  # if user_input is empty or None
            return None
        else:
            print(f"Invalid interval '{user_input}', using no interval.")
            return None

    # def fill_gaps_in_data(self, df, contract, app):
    #     """
    #     Fills gaps in historical data by fetching the missing data from the API.
    #     """
    #     ny_tz = pytz.timezone('America/New_York')
    #     # df['Date'] = pd.to_datetime(df['Date'])
    #     # df['Date'] = df['Date'].dt.tz_localize(ny_tz, nonexistent='shift_forward', ambiguous='NaT')
    #     df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    #     df['Date'] = df['Date'].dt.tz_localize(ny_tz, nonexistent='shift_forward', ambiguous='NaT')
    #     df = df.dropna(subset=['Date'])
    #
    #     df = df.set_index('Date')
    #
    #     # Δημιουργούμε μια σειρά από ημερομηνίες χωρίς κενά, σε ζώνη ώρας Νέας Υόρκης
    #     all_times = pd.date_range(start=df.index.min(), end=df.index.max(), freq='1min', tz=ny_tz)
    #
    #     # Εντοπίζουμε τα κενά
    #     missing_times = all_times.difference(df.index)
    #
    #     if not missing_times.empty:
    #         print(f"Found missing data for {contract.symbol} from {missing_times[0]} to {missing_times[-1]}.")
    #
    #         for missing_time in missing_times:
    #             start_time = missing_time - timedelta(minutes=2)
    #             end_time = missing_time + timedelta(minutes=2)
    #
    #             # Σωστή μετατροπή σε UTC για το API request (αν απαιτείται από το API)
    #             # start_time_utc = start_time.astimezone(pytz.utc).strftime('%Y%m%d %H:%M:%S')
    #             # end_time_utc = end_time.astimezone(pytz.utc).strftime('%Y%m%d %H:%M:%S')
    #
    #             app.update_minute_data_for_symbol(contract)
    #
    #             # Μετά την κλήση, ενημερώνουμε το DataFrame με τα νέα δεδομένα
    #             new_data = self.fetch_data_from_db('minute_data', start_time.strftime('%Y-%m-%d %H:%M:%S'),
    #                                                end_time.strftime('%Y-%m-%d %H:%M:%S'), ticker=contract.symbol)
    #
    #             if not new_data.empty:
    #                 df = pd.concat([df, new_data])
    #     df = df.sort_index()
    #     df = df.reset_index()
    #     return df

    @staticmethod
    def resample_data(df, interval):
        resampled_df = df.resample(interval).agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'Ticker': 'first'
        }).dropna().reset_index()

        return resampled_df

    def update_plot(self, contract, days=7,  interval_entry=None, interval_exit=None):
        real_time_data = self.process_queue_data()

        # Εκτύπωση της λίστας real_time_data πριν τη δημιουργία της DataFrame
        print("Real-time Data List:")
        print(self.real_time_data)

        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date, ticker=contract.symbol)
        df_minute.set_index('Date', inplace=True)

        # print("Minute data from DB:")
        # print(df_minute.tail())
        #
        # print("Minute data DataFrame with datetime check:")
        # print(df_minute.dtypes)

        ticker = contract.symbol

        # filtered_real_time_data = [row for row in self.real_time_data.get(ticker, [])]
        #
        # if filtered_real_time_data and all(
        #         row[1:] != [None, None, None, None, None] and row[1:] != [0.0, 0.0, 0.0, 0.0, 0.0] for row in
        #         filtered_real_time_data):
        real_time_df = pd.DataFrame(real_time_data,
                                    columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker'])
        real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')

        print("Real time df: ")
        print(real_time_df)

        # if ticker in real_time_data and not real_time_df.empty:
        #     real_time_df = real_time_data[ticker]
        # else:
        #     real_time_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # print("Real-time DataFrame with datetime check:")
        # print(real_time_df.dtypes)

        # print("Real-time DataFrame")
        # print(real_time_df)

        if 'Date' in real_time_df.columns:
            real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')  # Βεβαιώσου ότι είναι datetime
            real_time_df.set_index('Date', inplace=True)
            print("Valid real-time data found, DataFrame created:")
            print(real_time_df)
        else:
            print("Column 'Date' is missing, cannot set as index")
            print(real_time_df.columns)

        # real_time_df['Ticker'] = contract.symbol
        print("Valid real-time data found, DataFrame created:")
        print(real_time_df)
        # else:
        #     print("All real-time data rows are invalid, creating an empty DataFrame.")
        #     real_time_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Εκτύπωση των real-time δεδομένων για έλεγχο
        # print("Real-time DataFrame before combining:")
        # print(real_time_df.tail())

        dataframes_to_concat = [df for df in [df_minute, real_time_df] if not df.empty]
        combined_data = pd.concat(dataframes_to_concat)
        # print('Combined Data right after combination')
        # print(combined_data.tail())

        if not combined_data.empty:
        #     combined_data.reset_index(drop=False, inplace=True)
            if 'Date' in combined_data.columns:
                combined_data['Date'] = pd.to_datetime(combined_data['Date'], errors='coerce')

            if 'Date' in combined_data.columns:
                combined_data.set_index('Date', inplace=True)

            combined_data.sort_values(by='Date', inplace=True)
            print('Combined Data')
            print(combined_data.tail())
            #
            # print("Df_entry resampling")
            df_entry = self.resample_data(combined_data, interval_entry)
            # print(df_entry.tail())
            # print("Df_entry indicators")
            df_entry = self.calculate_indicators(df_entry)
            # print(df_entry.tail())

        # Resample and process for exit signals
        #     print("Df_exit resampling")
            df_exit = self.resample_data(combined_data, interval_exit)
            # print(df_exit.tail())
            # print("Df_exit indicators")
            df_exit = self.calculate_indicators(df_exit)
            # print(df_exit.tail())

            # Generate signals for entry and exit
            # print("Df_entry signals")
            df_entry = self.generate_signals(df_entry)
            # print(df_entry.tail())
            # print("Df_exit indicators")
            df_exit = self.generate_signals(df_exit)
            # print(df_exit.tail())

            # print(f"Exit {interval_exit} Data with Signals:")
            # print(df_exit.tail())
            #
            # print(f"Entry {interval_entry} Data with Signals:")
            # print(df_entry.tail())
            # pd.set_option('display.max_rows', 1000)
            # pd.set_option('display.max_columns', 10)
            # pd.set_option('display.width', 1000)

        # Εκτύπωση των 2 πρώτων και των 4 τελευταίων στηλών για το df_entry
            print(
                f"Entry {interval_entry} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
            print(df_entry.iloc[:, :2].join(df_entry.iloc[:, -4:]))

            # Εκτύπωση των 2 πρώτων και των 4 τελευταίων στηλών για το df_exit
            print(f"Exit {interval_exit} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
            print(df_exit.iloc[:, :2].join(df_exit.iloc[:, -4:]))

            # Export dataframes to Excel files with different names based on the interval
            self.export_to_excel(df_entry, filename=f"signals_{interval_entry}.xlsx")
            self.export_to_excel(df_exit, filename=f"signals_{interval_exit}.xlsx")

            return df_entry, df_exit

        else:
            print("No data to process.")
            return None


        # if not real_time_df.empty:
        #     print("Real-time data found!")
        #     real_time_df['Date'] = pd.to_datetime(real_time_df['Date'], errors='coerce')
        #     real_time_df.set_index('Date', inplace=True)
        #     print(real_time_df)

        # dataframes_to_concat = [df for df in [df_minute, real_time_df] if not df.empty and df['Date'].notna().all()]
        # combined_data = pd.concat(dataframes_to_concat)
        # print(combined_data)
        #
        # if not combined_data.empty:
        #     if 'Date' in combined_data.columns:
        #         combined_data['Date'] = pd.to_datetime(combined_data['Date'], errors='coerce')
        #         combined_data.set_index('Date', inplace=True)
        #
        #     df_entry = self.resample_data(combined_data, interval_entry)
        #     df_entry = self.calculate_indicators(df_entry)
        #
        #     df_exit = self.resample_data(combined_data, interval_exit)
        #     df_exit = self.calculate_indicators(df_exit)
        #
        #     df_entry = self.generate_signals(df_entry)
        #     df_exit = self.generate_signals(df_exit)
        #
        #     print(
        #         f"Entry {interval_entry} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
        #     print(df_entry.iloc[:, :2].join(df_entry.iloc[:, -4:]))
        #
        #     print(
        #         f"Exit {interval_exit} Data for {contract.symbol} with Signals (2 πρώτες στήλες και 4 τελευταίες στήλες):")
        #     print(df_exit.iloc[:, :2].join(df_exit.iloc[:, -4:]))
        #
        #     self.export_to_excel(df_entry, filename=f"signals_{interval_entry}.xlsx")
        #     self.export_to_excel(df_exit, filename=f"signals_{interval_exit}.xlsx")
        #
        #     return df_entry, df_exit
        # else:
        #     print("No data to process.")
        #     return None

    @staticmethod
    def export_to_excel(df, filename="output.xlsx"):
        # print(f"Exporting to Excel. Data:\n{df.tail()}")
        if 'Date' in df.columns:
            df['Date'] = df['Date'].dt.tz_localize(None)
        df.to_excel(filename, index=False)
        #
        # signals_df1 = self.generate_signals(df_stock1)
        # signals_df2 = self.generate_signals(df_stock2)
        #
        # # Αποθήκευση σε διαφορετικά sheets σε Excel
        # with pd.ExcelWriter('signals_output.xlsx') as writer:
        #     signals_df1.to_excel(writer, sheet_name='Stock1_Signals', index=False)
        #     signals_df2.to_excel(writer, sheet_name='Stock2_Signals', index=False)
