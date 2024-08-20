import queue
from datetime import datetime, timedelta, time
import pandas as pd
from order_manager import OrderManager


class DataProcessor:
    def __init__(self, db):
        self.db = db
        # self.app = app
        self.real_time_data = []
        self.interval = None
        self.data_ready_queue = queue.Queue()
        self.in_long_position = False
        self.in_short_position = False
        self.place_orders_outside_rth = False
        self.order_manager = OrderManager()

    def process_queue_data(self):
        while not self.data_ready_queue.empty():
            data = self.data_ready_queue.get()
            self.real_time_data.append(data)

    def fetch_data_from_db(self, table_name, start_date=None, end_date=None):
        return self.db.fetch_data_from_db(table_name, start_date, end_date)

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

    # @staticmethod
    # # 8 Columns for entry and exit for each position where if one of the criteria is true then it says true and
    # def generate_signals(df):
    #     # Initialize the signals columns
    #     df['Long_Entry'] = False
    #     df['Long_Entry_Criteria'] = ""
    #     df['Short_Entry'] = False
    #     df['Short_Entry_Criteria'] = ""
    #     df['Long_Exit'] = False
    #     df['Long_Exit_Criteria'] = ""
    #     df['Short_Exit'] = False
    #     df['Short_Exit_Criteria'] = ""
    #
    #     in_long_position = False
    #     in_short_position = False
    #
    #     for i in range(1, len(df)):
    #         if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(df['EMA20'].iloc[i]) or pd.isna(
    #                 df['EMA200'].iloc[i]):
    #             continue
    #
    #         long_entry_conditions = []
    #         short_entry_conditions = []
    #
    #         # Check Long Entry criteria
    #         if df['EMA9'].iloc[i] > df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] <= df['EMA20'].iloc[i - 1]:
    #             long_entry_conditions.append("EMA9 crossed above EMA20")
    #         if df['EMA9'].iloc[i] > df['EMA200'].iloc[i]:
    #             long_entry_conditions.append("EMA9 above EMA200")
    #         if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
    #             long_entry_conditions.append("EMA20 above EMA200")
    #         if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
    #             long_entry_conditions.append("Close above VWAP")
    #         if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] <= df['MACD_Signal'].iloc[
    #             i - 1]:
    #             long_entry_conditions.append("MACD crossed above Signal")
    #         if df['MACD'].iloc[i] > 0 and df['MACD'].iloc[i - 1] <= 0:
    #             long_entry_conditions.append("MACD crossed above 0")
    #         if df['Close'].iloc[i] < df['BB_Lower'].iloc[i] and df['Close'].iloc[i - 1] >= df['BB_Lower'].iloc[i - 1]:
    #             long_entry_conditions.append("Price crossed above BB Lower")
    #
    #         if long_entry_conditions:
    #             df.at[i, 'Long_Entry'] = True
    #             df.at[i, 'Long_Entry_Criteria'] = ', '.join(long_entry_conditions)
    #             in_long_position = True
    #
    #         # Check Short Entry criteria
    #         if df['EMA9'].iloc[i] < df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] >= df['EMA20'].iloc[i - 1]:
    #             short_entry_conditions.append("EMA9 crossed below EMA20")
    #         if df['EMA9'].iloc[i] < df['EMA200'].iloc[i]:
    #             short_entry_conditions.append("EMA9 below EMA200")
    #         if df['EMA20'].iloc[i] < df['EMA200'].iloc[i]:
    #             short_entry_conditions.append("EMA20 below EMA200")
    #         if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
    #             short_entry_conditions.append("Close below VWAP")
    #         if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] >= df['MACD_Signal'].iloc[
    #             i - 1]:
    #             short_entry_conditions.append("MACD crossed below Signal")
    #         if df['MACD'].iloc[i] < 0 and df['MACD'].iloc[i - 1] >= 0:
    #             short_entry_conditions.append("MACD crossed below 0")
    #         if df['Close'].iloc[i] > df['BB_Upper'].iloc[i] and df['Close'].iloc[i - 1] <= df['BB_Upper'].iloc[i - 1]:
    #             short_entry_conditions.append("Price crossed below BB Upper")
    #
    #         if short_entry_conditions:
    #             df.at[i, 'Short_Entry'] = True
    #             df.at[i, 'Short_Entry_Criteria'] = ', '.join(short_entry_conditions)
    #             in_short_position = True
    #
    #         # Check Long Exit criteria if in long position
    #         if in_long_position:
    #             long_exit_conditions = []
    #             if df['EMA9'].iloc[i] < df['EMA20'].iloc[i]:
    #                 long_exit_conditions.append("EMA9 below EMA20")
    #             if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
    #                 long_exit_conditions.append("Close below VWAP")
    #             if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i]:
    #                 long_exit_conditions.append("MACD below Signal")
    #             if df['Close'].iloc[i] >= df['BB_Upper'].iloc[i]:
    #                 long_exit_conditions.append("Price touches BB Upper")
    #
    #             if long_exit_conditions:
    #                 df.at[i, 'Long_Exit'] = True
    #                 df.at[i, 'Long_Exit_Criteria'] = ', '.join(long_exit_conditions)
    #                 in_long_position = False
    #
    #         # Check Short Exit criteria if in short position
    #         if in_short_position:
    #             short_exit_conditions = []
    #             if df['EMA9'].iloc[i] > df['EMA20'].iloc[i]:
    #                 short_exit_conditions.append("EMA9 above EMA20")
    #             if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
    #                 short_exit_conditions.append("Close above VWAP")
    #             if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i]:
    #                 short_exit_conditions.append("MACD above Signal")
    #             if df['Close'].iloc[i] <= df['BB_Lower'].iloc[i]:
    #                 short_exit_conditions.append("Price touches BB Lower")
    #
    #             if short_exit_conditions:
    #                 df.at[i, 'Short_Exit'] = True
    #                 df.at[i, 'Short_Exit_Criteria'] = ', '.join(short_exit_conditions)
    #                 in_short_position = False
    #
    #     return df

    @staticmethod
    def generate_signals(df):
        required_columns = ['EMA9', 'EMA20', 'EMA200', 'VWAP', 'MACD', 'MACD_Signal', 'BB_Upper', 'BB_Lower']
        for col in required_columns:
            if col not in df.columns:
                print(f"Not enough data to calculate {col}")
                return df

        # Initialize the signals columns
        criteria_columns = [
            'EMA9_above_EMA20', 'EMA9_below_EMA20', 'EMA9_above_EMA200', 'EMA9_below_EMA200',
            'EMA20_above_EMA200', 'EMA20_below_EMA200', 'Close_above_VWAP', 'Close_below_VWAP',
            'MACD_above_Signal', 'MACD_below_Signal', 'MACD_above_zero', 'MACD_below_zero',
            'Price_crossed_above_BB_Lower', 'Price_crossed_below_BB_Upper', 'Price_touches_BB_Upper',
            'Price_touches_BB_Lower'
        ]

        for col in criteria_columns:
            if col not in df.columns:
                df[col] = ""

        df['Long_Entry'] = False
        df['Short_Entry'] = False
        df['Long_Exit'] = False
        df['Short_Exit'] = False

        in_long_position = False
        in_short_position = False

        for i in range(1, len(df)):
            try:
                if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(
                        df['EMA20'].iloc[i]) or pd.isna(df['EMA200'].iloc[i]):
                    print(f"Skipping index {i} due to NaN values")
                    continue

                if i >= len(df):
                    print(f"Skipping index {i} because it is out of range")
                    continue

                # Long Entry Criteria
                long_entry_conditions = []
                if df['EMA9'].iloc[i] > df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] <= df['EMA20'].iloc[i - 1]:
                    long_entry_conditions.append("EMA9 crossed above EMA20")
                    df.at[i, 'EMA9_above_EMA20'] += "Long Entry, "
                if df['EMA9'].iloc[i] > df['EMA200'].iloc[i]:
                    long_entry_conditions.append("EMA9 above EMA200")
                    df.at[i, 'EMA9_above_EMA200'] += "Long Entry, "
                if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
                    long_entry_conditions.append("EMA20 above EMA200")
                    df.at[i, 'EMA20_above_EMA200'] += "Long Entry, "
                if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                    long_entry_conditions.append("Close above VWAP")
                    df.at[i, 'Close_above_VWAP'] += "Long Entry, "
                if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] <= df['MACD_Signal'].iloc[
                    i - 1]:
                    long_entry_conditions.append("MACD crossed above Signal")
                    df.at[i, 'MACD_above_Signal'] += "Long Entry, "
                if df['MACD'].iloc[i] > 0 and df['MACD'].iloc[i - 1] <= 0:
                    long_entry_conditions.append("MACD crossed above 0")
                    df.at[i, 'MACD_above_zero'] += "Long Entry, "
                if df['Close'].iloc[i] < df['BB_Lower'].iloc[i] and df['Close'].iloc[i - 1] >= df['BB_Lower'].iloc[
                    i - 1]:
                    long_entry_conditions.append("Price crossed above BB Lower")
                    df.at[i, 'Price_crossed_above_BB_Lower'] += "Long Entry, "

                if long_entry_conditions:
                    df.at[i, 'Long_Entry'] = True
                    in_long_position = True

                # Short Entry Criteria
                short_entry_conditions = []
                if df['EMA9'].iloc[i] < df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] >= df['EMA20'].iloc[i - 1]:
                    short_entry_conditions.append("EMA9 crossed below EMA20")
                    df.at[i, 'EMA9_below_EMA20'] += "Short Entry, "
                if df['EMA9'].iloc[i] < df['EMA200'].iloc[i]:
                    short_entry_conditions.append("EMA9 below EMA200")
                    df.at[i, 'EMA9_below_EMA200'] += "Short Entry, "
                if df['EMA20'].iloc[i] < df['EMA200'].iloc[i]:
                    short_entry_conditions.append("EMA20 below EMA200")
                    df.at[i, 'EMA20_below_EMA200'] += "Short Entry, "
                if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                    short_entry_conditions.append("Close below VWAP")
                    df.at[i, 'Close_below_VWAP'] += "Short Entry, "
                if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] >= df['MACD_Signal'].iloc[
                    i - 1]:
                    short_entry_conditions.append("MACD crossed below Signal")
                    df.at[i, 'MACD_below_Signal'] += "Short Entry, "
                if df['MACD'].iloc[i] < 0 and df['MACD'].iloc[i - 1] >= 0:
                    short_entry_conditions.append("MACD crossed below 0")
                    df.at[i, 'MACD_below_zero'] += "Short Entry, "
                if df['Close'].iloc[i] > df['BB_Upper'].iloc[i] and df['Close'].iloc[i - 1] <= df['BB_Upper'].iloc[
                    i - 1]:
                    short_entry_conditions.append("Price crossed below BB Upper")
                    df.at[i, 'Price_crossed_below_BB_Upper'] += "Short Entry, "

                if short_entry_conditions:
                    df.at[i, 'Short_Entry'] = True
                    in_short_position = True

                # Long Exit Criteria
                long_exit_conditions = []
                if in_long_position:
                    if df['EMA9'].iloc[i] < df['EMA20'].iloc[i]:
                        long_exit_conditions.append("EMA9 below EMA20")
                        df.at[i, 'EMA9_below_EMA20'] += "Long Exit, "
                    if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                        long_exit_conditions.append("Close below VWAP")
                        df.at[i, 'Close_below_VWAP'] += "Long Exit, "
                    if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i]:
                        long_exit_conditions.append("MACD below Signal")
                        df.at[i, 'MACD_below_Signal'] += "Long Exit, "
                    if df['Close'].iloc[i] >= df['BB_Upper'].iloc[i]:
                        long_exit_conditions.append("Price touches BB Upper")
                        df.at[i, 'Price_touches_BB_Upper'] += "Long Exit, "

                    if long_exit_conditions:
                        df.at[i, 'Long_Exit'] = True
                        in_long_position = False

                # Short Exit Criteria
                short_exit_conditions = []
                if in_short_position:
                    if df['EMA9'].iloc[i] > df['EMA20'].iloc[i]:
                        short_exit_conditions.append("EMA9 above EMA20")
                        df.at[i, 'EMA9_above_EMA20'] += "Short Exit, "
                    if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                        short_exit_conditions.append("Close above VWAP")
                        df.at[i, 'Close_above_VWAP'] += "Short Exit, "
                    if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i]:
                        short_exit_conditions.append("MACD above Signal")
                        df.at[i, 'MACD_above_Signal'] += "Short Exit, "
                    if df['Close'].iloc[i] <= df['BB_Lower'].iloc[i]:
                        short_exit_conditions.append("Price touches BB Lower")
                        df.at[i, 'Price_touches_BB_Lower'] += "Short Exit, "

                    if short_exit_conditions:
                        df.at[i, 'Short_Exit'] = True
                        in_short_position = False

            except KeyError as e:
                print(f"KeyError at index {i}: {e}, skipping this index.")
                continue

            except Exception as e:
                print(f"Unexpected error at index {i}: {e}, skipping this index.")
                continue

        for col in criteria_columns:
            df[col] = df[col].str.strip(', ')

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

    @staticmethod
    def resample_data(df, interval):
        resampled_df = df.resample(interval, on='Date').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna().reset_index()

        return resampled_df

    def update_plot(self, days=2, interval=None):
        self.process_queue_data()

        # Εκτύπωση της λίστας real_time_data πριν τη δημιουργία της DataFrame
        # print("Real-time Data List:")
        # print(self.real_time_data)

        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date)
        # print("Minute data from DB:")
        # print(df_minute.tail())
        real_time_df = pd.DataFrame(self.real_time_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Εκτύπωση των real-time δεδομένων για έλεγχο
        # print("Real-time DataFrame before combining:")
        # print(real_time_df.tail())

        combined_data = pd.concat([
            df_minute,
            real_time_df
        ])
        # print("Real-time Data combined:")
        # print(real_time_df.tail())

        combined_data['Date'] = pd.to_datetime(combined_data['Date'])
        combined_data.sort_values(by='Date', inplace=True)
        combined_data_ind = self.calculate_indicators(combined_data)

        # print("Combined Data ind:")
        # print(combined_data_ind.tail())

        if interval:
            resampled_data = self.resample_data(combined_data, interval)
            resampled_data = self.calculate_indicators(resampled_data)
            resampled_data = self.generate_signals(resampled_data)
        else:
            resampled_data = self.generate_signals(combined_data_ind)
        #
        # print("Combined Data:")
        # print(combined_data.tail())
        #
        # print("Resampled Data:")
        # print(resampled_data.tail())

        self.export_to_excel(resampled_data)
        return resampled_data

    @staticmethod
    def export_to_excel(df, filename="output.xlsx"):
        # print(f"Exporting to Excel. Data:\n{df.tail()}")
        df.to_excel(filename, index=False)
