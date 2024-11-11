import os
import sys
import json
import datetime
import yfinance as yf
import pandas as pd
from pprint import pprint
import logging
from logging.handlers import RotatingFileHandler
import traceback
import threading
import time


LOG_PATH = "loader.log"
LOG = logging.getLogger("Rotating Log")
LOG.setLevel(logging.INFO)
log_formatter = logging.Formatter(
    '[%(asctime)s][%(levelname)s] %(message)s')

handler = RotatingFileHandler(LOG_PATH,
                              maxBytes=1*1024*1024,
                              backupCount=2)

handler.setFormatter(log_formatter)
# handler = logging.StreamHandler(sys.stdout)
LOG.addHandler(handler)

DATA_PATH = "data"


def is_empty_dir(path):

    if os.path.isdir(path):
        files = os.listdir(path)
        if '_common_metadata' in files:
            files.remove('_common_metadata')
        if '_metadata' in files:
            files.remove('_metadata')
        if len(files) > 0:
            return False
    return True


# https://localhost:5000/api/call/load_stock?symbol="MSFT"&_output_format=str
def load_stock(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/stocks/{symbol}"

    print(f"Loanding stock : {symbol}")
    ticker = yf.Ticker(symbol)

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = ticker.history(start=last_update.strftime('%Y-%m-%d'))
    data = data[data.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
                    "partition"], append=append_mode)


# https://localhost:5000/api/call/stock?symbol="MSFT"&start="2024-11-05"&_output_format=str
def stock(symbol: str = "MSFT",
          start: str = "1900-01-01",
          end: str = "3000-01-01") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/stocks/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data = data[(data.index >= start) & (data.index <= end)]

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_dividends?symbol="MSFT"&_output_format=str
def load_dividends(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/dividends/{symbol}"
    LOG.info(f"Loanding dividends : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.dividends.to_frame()

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime('%Y-%m-%d')]
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/dividends?symbol="MSFT"&_output_format=str
def dividends(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/dividends/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_splits?symbol="MSFT"&_output_format=str
def load_splits(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/splits/{symbol}"
    print(f"Loanding splits : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.splits.to_frame()

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/splits?symbol="MSFT"&_output_format=str
def splits(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/splits/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_income?symbol="MSFT"&_output_format=str
def load_income(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/income_stmt/{symbol}"
    print(f"Loanding income_stmt : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.income_stmt.transpose()
    data.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime('%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/income?symbol="MSFT"&_output_format=str
def income(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/income_stmt/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_quarterly_income?symbol="MSFT"&_output_format=str
def load_quarterly_income(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/quarterly_income_stmt/{symbol}"
    print(f"Loanding quarterly_income_stmt : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.quarterly_income_stmt.transpose()
    data.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime(
        '%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/quarterly_income?symbol="MSFT"&_output_format=str
def quarterly_income(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/quarterly_income_stmt/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime(
        '%Y-%m-%d')
    return data


# https://localhost:5000/api/call/SP500&_output_format=str
def SP500() -> pd.core.frame.DataFrame:

    list_sp500 = pd.read_csv(
        'https://datahub.io/core/s-and-p-500-companies/_r/-/data/constituents.csv')
    list_sp500['Symbol'] = list_sp500['Symbol'].str.replace('.', '-')
    return list_sp500


# https://localhost:5000/api/call/load_cashflow?symbol="MSFT"&_output_format=str
def load_cashflow(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/cashflow/{symbol}"
    print(f"Loanding cashflow : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.cashflow.transpose()
    data.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime(
        '%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/cashflow?symbol="MSFT"&_output_format=str
def cashflow(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/cashflow/{symbol}"
    df_cashflow = pd.read_parquet(path, engine='fastparquet')
    df_cashflow.set_index("Date", inplace=True)

    df_cashflow.reset_index(drop=False, inplace=True)
    df_cashflow["Date"] = df_cashflow["Date"].dt.strftime(
        '%Y-%m-%d')
    return df_cashflow


# https://localhost:5000/api/call/load_quarterly_cashflow?symbol="MSFT"&_output_format=str
def load_quarterly_cashflow(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/quarterly_cashflow/{symbol}"
    print(f"Loanding quarterly_cashflow : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.quarterly_cashflow.transpose()
    data.index.names = ['Date']

    if is_empty_dir(path):
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    else:
        if os.path.isfile(path+"/_metadata"):
            os.remove(path+"/_metadata")
        last_update = pd.read_parquet(
            path, engine='fastparquet', columns=["Date"])["Date"].max()
        append_mode = True

    data = data[data.index > last_update.strftime(
        '%Y-%m-%d')]
    print(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    print(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/quarterly_cashflow?symbol="MSFT"&_output_format=str
def quarterly_cashflow(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/quarterly_cashflow/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime(
        '%Y-%m-%d')
    return data


def loader_SP500():
    for symbol in list(SP500()["Symbol"]):
        # load_stock(symbol)
        load_dividends(symbol)
        # load_splits(symbol)
        # load_income(symbol)
        # load_quarterly_income(symbol)
        # load_cashflow(symbol)
        # load_quarterly_cashflow(symbol)


# load_stock()
# data = stock(start="2024-11-05")

# load_dividends()
# pprint(dividends())

# load_splits()
# pprint(splits())

# load_income()
# pprint(income())
# load_quarterly_income()
# pprint(quarterly_income())

# loader_SP500()


def scheduler():
    cpt = 5
    while True:
        try:
            t = 5/cpt
            cpt -= 1
            LOG.info("I'm running")
            print("I'm running")
            time.sleep(1)
        except Exception as e:
            exception = traceback.format_exc()
            LOG.error(exception)
            time.sleep(10)


threading.Thread(target=scheduler).start()


while True:
    print("exec api")
    time.sleep(2)


#
#    msft = yf.Ticker("MSFT")
#    pprint(msft.calendar)

# TODO: a finir
# msft.balance_sheet
# msft.quarterly_balance_sheet
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
# les taux de la fed 
# les taux de la BCE
# le cours de l'or  


# Informations:
# le s&p 500 represent 80% du marché américain
# le états unis produise 25% du PIB mondial
# la capitalisation du cac 40 = 2 460 milliards d'euro en décembre 2023
# la sum des capitalisation du S&P et de 50 000 milliard
