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
    finally:
        session.close()
def main_menu():
    while True:
        print("\nMenu:")
        print("1. Add Tickers")
        print("2. Delete Ticker")
        print("3. Exit")
        choice = input("Choose 1, 2 or 3: ")
        if choice == '1':
            tickers = get_tickers_from_user()
            insert_tickers(tickers)
        elif choice == '2':
            delete_ticker()
        elif choice == '3':
            print("Exiting...")
            break
        else:
            print("No valid choice.")
if __name__ == "__main__":
    main_menu()