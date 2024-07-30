import queue
from datetime import datetime, timedelta

import pandas as pd

class DataProcessor:
    def __init__(self, db):
        self.db = db
        self.real_time_data = []
        self.interval = None
        self.data_ready_queue = queue.Queue()

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

    @staticmethod
    def generate_signals(df):
        required_columns = ['EMA9', 'EMA20', 'EMA200', 'VWAP', 'MACD', 'MACD_Signal', 'BB_Upper', 'BB_Lower']
        for col in required_columns:
            if col not in df.columns:
                print(f"Not enough data to calculate {col}")
                return df

        criteria_columns = [
            'EMA9_above_EMA20', 'EMA9_below_EMA20', 'EMA9_above_EMA200', 'EMA9_below_EMA200',
            'EMA20_above_EMA200', 'EMA20_below_EMA200', 'Close_above_VWAP', 'Close_below_VWAP',
            'MACD_above_Signal', 'MACD_below_Signal', 'MACD_above_zero', 'MACD_below_zero',
            'Price_crossed_above_BB_Lower', 'Price_crossed_below_BB_Upper', 'Price_touches_BB_Upper',
            'Price_touches_BB_Lower'
        ]

        for col in criteria_columns:
            df[col] = ""

        df['Long_Entry'] = False
        df['Short_Entry'] = False
        df['Long_Exit'] = False
        df['Short_Exit'] = False

        in_long_position = False
        in_short_position = False

        for i in range(1, len(df)):
            if pd.isna(df['Close'].iloc[i]) or pd.isna(df['EMA9'].iloc[i]) or pd.isna(df['EMA20'].iloc[i]) or pd.isna(
                    df['EMA200'].iloc[i]):
                continue

            if df['EMA9'].iloc[i] > df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] <= df['EMA20'].iloc[i - 1]:
                df.at[i, 'EMA9_above_EMA20'] += "Long Entry, "
            if df['EMA9'].iloc[i] > df['EMA200'].iloc[i]:
                df.at[i, 'EMA9_above_EMA200'] += "Long Entry, "
            if df['EMA20'].iloc[i] > df['EMA200'].iloc[i]:
                df.at[i, 'EMA20_above_EMA200'] += "Long Entry, "
            if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                df.at[i, 'Close_above_VWAP'] += "Long Entry, "
            if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] <= df['MACD_Signal'].iloc[i - 1]:
                df.at[i, 'MACD_above_Signal'] += "Long Entry, "
            if df['MACD'].iloc[i] > 0 and df['MACD'].iloc[i - 1] <= 0:
                df.at[i, 'MACD_above_zero'] += "Long Entry, "
            if df['Close'].iloc[i] < df['BB_Lower'].iloc[i] and df['Close'].iloc[i - 1] >= df['BB_Lower'].iloc[i - 1]:
                df.at[i, 'Price_crossed_above_BB_Lower'] += "Long Entry, "

            if df['EMA9'].iloc[i] < df['EMA20'].iloc[i] and df['EMA9'].iloc[i - 1] >= df['EMA20'].iloc[i - 1]:
                df.at[i, 'EMA9_below_EMA20'] += "Short Entry, "
            if df['EMA9'].iloc[i] < df['EMA200'].iloc[i]:
                df.at[i, 'EMA9_below_EMA200'] += "Short Entry, "
            if df['EMA20'].iloc[i] < df['EMA200'].iloc[i]:
                df.at[i, 'EMA20_below_EMA200'] += "Short Entry, "
            if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                df.at[i, 'Close_below_VWAP'] += "Short Entry, "
            if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i] and df['MACD'].iloc[i - 1] >= df['MACD_Signal'].iloc[i - 1]:
                df.at[i, 'MACD_below_Signal'] += "Short Entry, "
            if df['MACD'].iloc[i] < 0 and df['MACD'].iloc[i - 1] >= 0:
                df.at[i, 'MACD_below_zero'] += "Short Entry, "
            if df['Close'].iloc[i] > df['BB_Upper'].iloc[i] and df['Close'].iloc[i - 1] <= df['BB_Upper'].iloc[i - 1]:
                df.at[i, 'Price_crossed_below_BB_Upper'] += "Short Entry, "

            if in_long_position:
                if df['EMA9'].iloc[i] < df['EMA20'].iloc[i]:
                    df.at[i, 'EMA9_below_EMA20'] += "Long Exit, "
                if df['Close'].iloc[i] < df['VWAP'].iloc[i]:
                    df.at[i, 'Close_below_VWAP'] += "Long Exit, "
                if df['MACD'].iloc[i] < df['MACD_Signal'].iloc[i]:
                    df.at[i, 'MACD_below_Signal'] += "Long Exit, "
                if df['Close'].iloc[i] >= df['BB_Upper'].iloc[i]:
                    df.at[i, 'Price_touches_BB_Upper'] += "Long Exit, "

            if in_short_position:
                if df['EMA9'].iloc[i] > df['EMA20'].iloc[i]:
                    df.at[i, 'EMA9_above_EMA20'] += "Short Exit, "
                if df['Close'].iloc[i] > df['VWAP'].iloc[i]:
                    df.at[i, 'Close_above_VWAP'] += "Short Exit, "
                if df['MACD'].iloc[i] > df['MACD_Signal'].iloc[i]:
                    df.at[i, 'MACD_above_Signal'] += "Short Exit, "
                if df['Close'].iloc[i] <= df['BB_Lower'].iloc[i]:
                    df.at[i, 'Price_touches_BB_Lower'] += "Short Exit, "

            df.at[i, 'Long_Entry'] = "Long Entry" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', '))
            df.at[i, 'Short_Entry'] = "Short Entry" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', '))
            df.at[i, 'Long_Exit'] = "Long Exit" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', ')) and in_long_position
            df.at[i, 'Short_Exit'] = "Short Exit" in df.iloc[i][criteria_columns].apply(lambda x: x.strip(', ')) and in_short_position

            if df.at[i, 'Long_Entry']:
                in_long_position = True
            if df.at[i, 'Short_Entry']:
                in_short_position = True
            if df.at[i, 'Long_Exit']:
                in_long_position = False
            if df.at[i, 'Short_Exit']:
                in_short_position = False

        for col in criteria_columns:
            df[col] = df[col].str.strip(', ')

        df = df.drop(columns=['Long_Entry', 'Short_Entry', 'Long_Exit', 'Short_Exit'])

        return df

    def validate_interval(self, user_input):
        valid_intervals = ['1min', '2min', '3min', '4min', '5min', '10min', '15min', '30min', '1H']
        if user_input in valid_intervals:
            return user_input
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

    def update_plot(self, days=3, interval=None):
        self.process_queue_data()  # Νέα μέθοδος για την επεξεργασία της ουράς πριν από την ενημέρωση του διαγράμματος

        # Εκτύπωση της λίστας real_time_data πριν τη δημιουργία της DataFrame
        print("Real-time Data List:")
        print(self.real_time_data)

        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

        df_minute = self.fetch_data_from_db('minute_data', start_date, end_date)
        real_time_df = pd.DataFrame(self.real_time_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Εκτύπωση των real-time δεδομένων για έλεγχο
        print("Real-time DataFrame before combining:")
        print(real_time_df.tail())

        combined_data = pd.concat([
            df_minute,
            real_time_df
        ])
        print("Real-time Data:")
        print(real_time_df.tail())

        combined_data['Date'] = pd.to_datetime(combined_data['Date'])
        combined_data.sort_values(by='Date', inplace=True)
        combined_data = self.calculate_indicators(combined_data)

        if interval:
            resampled_data = self.resample_data(combined_data, interval)
            resampled_data = self.calculate_indicators(resampled_data)
            resampled_data = self.generate_signals(resampled_data)
        else:
            resampled_data = combined_data

        print("Combined Data:")
        print(combined_data.tail())

        self.export_to_excel(resampled_data)
        return resampled_data

    @staticmethod
    def export_to_excel(df, filename="output.xlsx"):
        print(f"Exporting to Excel. Data:\n{df.tail()}")
        df.to_excel(filename, index=False)
