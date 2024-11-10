
import json
import yfinance as yf
import pandas as pd
import datetime
import os
from pprint import pprint

DATA_PATH = "data"


def is_empty_dir(path):
    if os.path.isdir(path) and os.listdir(path):
        return False
    return True

# https://localhost:5000/api/call/load_stock?symbol="MSFT"


def load_stock(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/stocks/{symbol}"

    print(f"Loanding stock : {symbol}")
    ticker = yf.Ticker(symbol)

    # get all stock info
    # ticker.info

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    hist = ticker.history(start=last_update.strftime('%Y-%m-%d'))
    hist = hist[hist.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(hist.shape[0])}")

    hist['partition'] = hist.index
    hist['partition'] = pd.Categorical(hist['partition'].dt.strftime('%Y-%m'))
    hist.reset_index(drop=False, inplace=True)

    hist.to_parquet(path,  engine='fastparquet', partition_cols=[
                    "partition"], append=append_mode)

    return True


# https://localhost:5000/api/call/stock?symbol="MSFT"&start="2024-11-05"
def stock(symbol: str = "MSFT",
          start: str = "1900-01-01",
          end: str = "3000-01-01") -> str:

    path = f"{DATA_PATH}/stocks/{symbol}"
    stock = pd.read_parquet(path, engine='fastparquet')
    stock.set_index("Date", inplace=True)

    stock = stock[(stock.index >= start) & (stock.index <= end)]

    stock.reset_index(drop=False, inplace=True)
    stock["Date"] = stock["Date"].dt.strftime('%Y-%m-%d')
    return stock.to_dict('list')


# https://localhost:5000/api/call/load_dividends?symbol="MSFT"
def load_dividends(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/dividends/{symbol}"
    print(f"Loanding dividends : {symbol}")
    ticket = yf.Ticker(symbol)

    df_div = ticket.dividends.to_frame()

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    df_div = df_div[df_div.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(df_div.shape[0])}")

    df_div['partition'] = df_div.index
    df_div['partition'] = pd.Categorical(
        df_div['partition'].dt.strftime('%Y-%m'))
    df_div.reset_index(drop=False, inplace=True)

    df_div.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)

    return True


# https://localhost:5000/api/call/dividends?symbol="MSFT"
def dividends(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/dividends/{symbol}"
    df_div = pd.read_parquet(path, engine='fastparquet')
    df_div.set_index("Date", inplace=True)

    df_div.reset_index(drop=False, inplace=True)
    df_div["Date"] = df_div["Date"].dt.strftime('%Y-%m-%d')
    return df_div.to_dict('list')


# https://localhost:5000/api/call/load_splits?symbol="MSFT"
def load_splits(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/splits/{symbol}"
    print(f"Loanding splits : {symbol}")
    ticket = yf.Ticker(symbol)

    df_div = ticket.splits.to_frame()

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    df_div = df_div[df_div.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(df_div.shape[0])}")

    df_div['partition'] = df_div.index
    df_div['partition'] = pd.Categorical(
        df_div['partition'].dt.strftime('%Y-%m'))
    df_div.reset_index(drop=False, inplace=True)

    df_div.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)

    return True


# https://localhost:5000/api/call/splits?symbol="MSFT"
def splits(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/splits/{symbol}"
    df_div = pd.read_parquet(path, engine='fastparquet')
    df_div.set_index("Date", inplace=True)

    df_div.reset_index(drop=False, inplace=True)
    df_div["Date"] = df_div["Date"].dt.strftime('%Y-%m-%d')
    return df_div.to_dict('list')


# https://localhost:5000/api/call/load_income?symbol="MSFT"
def load_income(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/income_stmt/{symbol}"
    print(f"Loanding income_stmt : {symbol}")
    ticket = yf.Ticker(symbol)

    df_income = ticket.income_stmt.transpose()
    df_income.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    df_income = df_income[df_income.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(df_income.shape[0])}")

    df_income['partition'] = df_income.index
    df_income['partition'] = pd.Categorical(
        df_income['partition'].dt.strftime('%Y-%m'))
    df_income.reset_index(drop=False, inplace=True)

    df_income.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)

    return True

# https://localhost:5000/api/call/income?symbol="MSFT"


def income(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/income_stmt/{symbol}"
    df_income = pd.read_parquet(path, engine='fastparquet')
    df_income.set_index("Date", inplace=True)

    df_income.reset_index(drop=False, inplace=True)
    df_income["Date"] = df_income["Date"].dt.strftime('%Y-%m-%d')
    return df_income.to_dict('list')


# https://localhost:5000/api/call/load_quarterly_income?symbol="MSFT"
def load_quarterly_income(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/quarterly_income_stmt/{symbol}"
    print(f"Loanding quarterly_income_stmt : {symbol}")
    ticket = yf.Ticker(symbol)

    df_quarterly_income = ticket.quarterly_income_stmt.transpose()
    df_quarterly_income.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    df_quarterly_income = df_quarterly_income[df_quarterly_income.index > last_update.strftime(
        '%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(df_quarterly_income.shape[0])}")

    df_quarterly_income['partition'] = df_quarterly_income.index
    df_quarterly_income['partition'] = pd.Categorical(
        df_quarterly_income['partition'].dt.strftime('%Y-%m'))
    df_quarterly_income.reset_index(drop=False, inplace=True)

    df_quarterly_income.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)

    return True


# https://localhost:5000/api/call/quarterly_income?symbol="MSFT"
def quarterly_income(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/quarterly_income_stmt/{symbol}"
    df_quarterly_income = pd.read_parquet(path, engine='fastparquet')
    df_quarterly_income.set_index("Date", inplace=True)

    df_quarterly_income.reset_index(drop=False, inplace=True)
    df_quarterly_income["Date"] = df_quarterly_income["Date"].dt.strftime(
        '%Y-%m-%d')
    return df_quarterly_income.to_dict('list')

# https://localhost:5000/api/call/SP500
def SP500()-> pd.core.frame.DataFrame:
    list_sp500 = pd.read_csv('https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv')
    list_sp500['Symbol'] = list_sp500['Symbol'].str.replace('.', '-')
    return list_sp500



# load_stock()
# data = stock(start="2024-11-05")

# load_dividends()
# pprint(dividends())

# load_splits()
# pprint(splits())

# load_income()
# pprint(income())
#load_quarterly_income()
#pprint(quarterly_income())


for symbol in list(SP500()["Symbol"]):
    load_stock(symbol)

exit(0)

msft = yf.Ticker("MSFT")

pprint(msft.calendar)

# TODO: a finir 

# liste du s&p 500 : https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv

#msft.balance_sheet
#msft.quarterly_balance_sheet
## - cash flow statement
#msft.cashflow
#msft.quarterly_cashflow
# msft.get_shares_full(start="2022-01-01", end=None)
# msft.calendar

# CA
# resultat net
# fiscalité et taxe
# resultat d'exploitation
# dette
# distripution des dividendes
# investisement
# cacheflow


# Informations:
# le s&p 500 represent 80% du marché américain
# le états unis produise 25% du PIB mondial
# la capitalisation du cac 40 = 2 460 milliards d'euro en décembre 2023
# la sum des capitalisation du S&P et de 50 000 milliard


# TODO: ajouter une option pour renvoyer la valeur binaire en base64
# base64.b64encode(pickle.dumps(["coucou", 6])).decode("ascii")
# pickle.loads(base64.b64decode('gASVEAAAAAAAAABdlCiMBmNvdWNvdZRLBmUu'.encode("ascii")))
