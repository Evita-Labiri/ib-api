import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# connection_string = f"mysql+mysqlconnector://{os.getenv('STOCKDATADB_UN')}:{os.getenv('STOCKDATADB_PASS')}@100.64.0.21/stockdatadb"
connection_string = f"mysql+mysqlconnector://{os.getenv('STOCKDATADB_UN')}:{os.getenv('STOCKDATADB_PASS')}@localhost/stockdatadb"
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)
session = Session()

def insert_tickers(ticker_list):
    try:
        current_date = datetime.now().date()
        for ticker in ticker_list:
            query = text("INSERT INTO daily_tickers (ticker, date) VALUES (:ticker, :date)")
            session.execute(query, {"ticker": ticker, "date": current_date})

        session.commit()
        print(f"Tickers added to the database with date: {current_date}")
    except Exception as e:
        session.rollback()
        print(f"Error with insertation: {e}")
    finally:
        session.close()

def get_tickers_from_user():
    ticker_list = input("Add ticker(s), separated with comma (π.χ. AAPL, GOOGL, TSLA): ").split(',')
    ticker_list = [ticker.strip().upper() for ticker in ticker_list]
    return ticker_list

def get_ticker_to_delete():
    ticker = input("Add the ticker(s) you want to delete: ").strip().upper()
    return ticker

def delete_ticker():
    try:
        ticker = get_ticker_to_delete()
        query = text("DELETE FROM daily_tickers WHERE ticker = :ticker")
        session.execute(query, {"ticker": ticker})
        session.commit()
        print(f"Ticker {ticker} successfully deleted from database.")
    except Exception as e:
        session.rollback()
        print(f"Error while deleting {ticker}: {e}")

def get_all_tickers():
    try:
        query = text("SELECT ticker, date FROM daily_tickers")
        results = session.execute(query).fetchall()
        if results:
            print("\nAll Tickers in the Database:")
            for row in results:
                print(f"Ticker: {row.ticker}, Date: {row.date}")
        else:
            print("No tickers found in the database.")
    except Exception as e:
        print(f"Error retrieving tickers: {e}")
    finally:
        session.close()

def get_todays_tickers():
    try:
        current_date = datetime.now().date()
        query = text("SELECT ticker FROM daily_tickers WHERE date = :date")
        results = session.execute(query, {"date": current_date}).fetchall()
        if results:
            print(f"\nTickers for today ({current_date}):")
            for row in results:
                print(f"Ticker: {row.ticker}")
        else:
            print("No tickers found for today in the database.")
    except Exception as e:
        print(f"Error retrieving today's tickers: {e}")
    finally:
        session.close()

def main_menu():
    while True:
        print("\nMenu:")
        print("1. Add Tickers")
        print("2. Delete Ticker")
        print("3. Show All Tickers")
        print("4. Show Today's Tickers")
        print("5. Exit")
        choice = input("Choose 1, 2, 3, 4 or 5: ")
        if choice == '1':
            tickers = get_tickers_from_user()
            insert_tickers(tickers)
        elif choice == '2':
            delete_ticker()
        elif choice == '3':
            get_all_tickers()
        elif choice == '4':
            get_todays_tickers()
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("No valid choice.")
if __name__ == "__main__":
    main_menu()