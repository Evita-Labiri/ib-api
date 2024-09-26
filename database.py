import os
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import pandas as pd

class Database:
    def __init__(self):
        self.engine = self.create_engine()
        if self.engine:
            self.Session = sessionmaker(bind=self.engine)
            self.session = self.Session()
        else:
            self.Session = None
            self.session = None

    def create_engine(self):
        try:
            connection_string = f"mysql+mysqlconnector://{os.getenv('STOCKDATADB_UN')}:{os.getenv('STOCKDATADB_PASS')}@100.64.0.21/stockdatadb"
            # connection_string = f"mysql+mysqlconnector://{os.getenv('STOCKDATADB_UN')}:{os.getenv('STOCKDATADB_PASS')}@localhost/stockdatadb"
            engine = create_engine(connection_string, connect_args={'connect_timeout': 28800})
            print("Successfully connected to the database with SQLAlchemy")
            return engine
        except Exception as e:
            print(f"Error while connecting to the database with SQLAlchemy: {e}")
            return None

    def ensure_connection(self):
        try:
            if self.engine is None:
                print("Database engine is not available, reconnecting...")
                self.engine = self.create_engine()
                if self.engine:
                    self.Session = sessionmaker(bind=self.engine)
                    self.session = self.Session()

            if self.session is None:
                print("Database session is not available, creating a new session...")
                self.session = self.Session()

            if self.session and not self.session.is_active:
                print("Database session is not active, creating a new session...")
                self.session = self.Session()
            else:
                print("Database session is active and connected.")
        except SQLAlchemyError as e:
            print(f"Error ensuring connection: {e}")
            if self.session:
                self.session.rollback()

    def insert_data_to_db(self, df, table_name):
        for index, row in df.iterrows():
            self.ensure_connection()
            try:
                # print(f"Inserting row: {row}")
                # print(f"Row keys: {row.keys()}")
                query = text(f"""
                              INSERT INTO {table_name} (ticker, datetime, open, high, low, close, volume)
                              VALUES (:ticker, :datetime, :open, :high, :low, :close, :volume)
                          """)
                self.session.execute(query, {
                    'ticker': row['ticker'],
                    'date_time': row['datetime'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
                self.session.commit()
            except SQLAlchemyError as e:
                print(f"Error inserting data into {table_name}: {e}")
                self.session.rollback()

    def insert_data_to_minute_table(self, table_name, ticker, date, open, high, low, close, volume):
        # ticker = "AAPL"
        try:
            self.ensure_connection()

            # Check if the ticker exists in the 'companies' table
            ticker_check_query = text("SELECT COUNT(*) FROM companies WHERE ticker = :ticker")
            ticker_result = self.session.execute(ticker_check_query, {'ticker': ticker})
            ticker_exists = ticker_result.fetchone()[0]

            if ticker_exists == 0:
                # If the ticker doesn't exist, insert it into the 'companies' table
                insert_ticker_query = text("INSERT INTO companies (ticker) VALUES (:ticker)")
                self.session.execute(insert_ticker_query, {'ticker': ticker})
                self.session.commit()
                print(f"Ticker {ticker} inserted into companies table.")

            query = text(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = :ticker AND date_time = :date_time")
            result = self.session.execute(query, {
                'ticker': ticker,
                'date_time': date
            })

            row = result.fetchone()
            if row and row[0] == 0:
                # print(f"Inserting minute data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                insert_query = text(f"""
                                INSERT INTO {table_name} (ticker, date_time, open, high, low, close, volume)
                                VALUES (:ticker, :date_time, :open, :high, :low, :close, :volume)
                            """)
                self.session.execute(insert_query, {
                    'ticker': ticker,
                    'date_time': date,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume
                })
                self.session.commit()
                print("Data inserted")
            else:
                print(f"Duplicate minute data found for {ticker} at {date}, skipping insertion.")
        except SQLAlchemyError as e:
            print(f"Error inserting minute data into {table_name}: {e}")
            self.session.rollback()

    def insert_data_to_daily_table(self, table_name, ticker, date, open, high, low, close, volume):
        # ticker = "AAPL"
        try:
            self.ensure_connection()
            query = text(f"SELECT COUNT(*) FROM {table_name} WHERE ticker = :ticker AND date = :date")
            result = self.session.execute(query, {
                'ticker': ticker,
                'date': date
            })

            row = result.fetchone()
            if row and row[0] == 0:
                print(f"Inserting daily data: {ticker}, {date}, {open}, {high}, {low}, {close}, {volume}")
                insert_query = text(f"""
                     INSERT INTO {table_name} (ticker, date, open, high, low, close, volume)
                     VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
                 """)
                self.session.execute(insert_query, {
                    'ticker': ticker,
                    'date': date,
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume
                })
                self.session.commit()
                print("Data inserted")
            else:
                print(f"Duplicate daily data found for {ticker} at {date}, skipping insertion.")
        except SQLAlchemyError as e:
            print(f"Error inserting daily data into {table_name}: {e}")
            self.session.rollback()

    def fetch_data_from_db(self, table_name, start_date=None, end_date=None, ticker=None):
        columns = self.fetch_table_columns(table_name)
        date_col = 'date_time' if 'date_time' in columns else 'Date'
        query = f"SELECT {date_col} as Date, open as Open, high as High, low as Low, close as Close, volume as Volume, ticker as Ticker FROM {table_name}"

        filters = []
        if ticker:
            filters.append(f"ticker = '{ticker}'")
        if start_date and end_date:
            filters.append(f"{date_col} BETWEEN '{start_date}' AND '{end_date}'")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        try:
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            print(f"Error fetching data from MySQL table {table_name}: {e}")
            return pd.DataFrame()

    def fetch_table_columns(self, table_name):
        query = f"SHOW COLUMNS FROM {table_name}"
        try:
            columns = pd.read_sql(query, self.engine)
            # print(f"Columns in {table_name}: {columns['Field'].tolist()}")
            return columns['Field'].tolist()
        except Exception as e:
            print(f"Error fetching columns from table {table_name}: {e}")
            return []

    def clear_data_from_table(self, table_name):
        try:
            self.ensure_connection()
            query = text(f"TRUNCATE TABLE {table_name}")
            self.session.execute(query)
            self.session.commit()
            print(f"All data from {table_name} has been deleted.")
        except SQLAlchemyError as e:
            print(f"Error deleting data from {table_name}: {e}")
            self.session.rollback()

    def update_data_in_db(self, df, table_name, temp_table_name):
        df.to_sql(temp_table_name, self.engine, if_exists='replace', index=False)
        # print("Data saved to temporary table successfully")

        with self.engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {table_name} t
                JOIN {temp_table_name} temp ON t.id = temp.id
                SET t.date_time = temp.date_time
            """))
        print("Data updated successfully")

    def load_data_from_db(self, table_name):
        # print(f"Attempting to load data from table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        # print(f"SQL Query: {query}")

        try:
            df = pd.read_sql(query, self.engine)
            print("Data loaded successfully")
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return pd.DataFrame()

    def get_last_date_for_symbol(self, ticker):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text("""
                        SELECT * FROM minute_data 
                        WHERE ticker = :ticker 
                        ORDER BY date_time DESC 
                        LIMIT 1
                    """),
                    {"ticker": ticker}
                ).scalar()

            if isinstance(result, int):
                result = datetime.fromtimestamp(result)
            elif isinstance(result, str):
                result = datetime.strptime(result, '%Y-%m-%d %H:%M:%S')

            return result

        except Exception as e:
            print(f"Error fetching last record for {ticker}: {e}")
            return None

    def db_close_connection(self):
        self.session.close()
        print("Database connection closed")