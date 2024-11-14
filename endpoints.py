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
import numpy as np
import requests
import configparser

pd.set_option('display.max_rows', None)


LOG_PATH = "loader.log"
LOG = logging.getLogger("Rotating Log")
LOG.setLevel(logging.INFO)
log_formatter = logging.Formatter(
    '[%(asctime)s][%(levelname)s] %(message)s')

handler = RotatingFileHandler(LOG_PATH,
                              maxBytes=1*1024*1024,
                              backupCount=2)

handler.setFormatter(log_formatter)
handler = logging.StreamHandler(sys.stdout)
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

    LOG.info(f"Loanding stock : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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
    LOG.info(f"Loanding splits : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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
    LOG.info(f"Loanding income_stmt : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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
    LOG.info(f"Loanding quarterly_income_stmt : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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
    LOG.info(f"Loanding cashflow : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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
    LOG.info(f"Loanding quarterly_cashflow : {symbol}")
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

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


# https://localhost:5000/api/call/load_balance_sheet?symbol="MSFT"&_output_format=str
def load_balance_sheet(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/balance_sheet/{symbol}"
    LOG.info(f"Loanding balance_sheet : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.balance_sheet.transpose()
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/balance_sheet?symbol="MSFT"&_output_format=str
def balance_sheet(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/balance_sheet/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime(
        '%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_quarterly_balance_sheet?symbol="MSFT"&_output_format=str
def load_quarterly_balance_sheet(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/quarterly_balance_sheet/{symbol}"
    LOG.info(f"Loanding quarterly_balance_sheet : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.quarterly_balance_sheet.transpose()
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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/quarterly_balance_sheet?symbol="MSFT"&_output_format=str
def quarterly_balance_sheet(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/quarterly_balance_sheet/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime(
        '%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_calendar?symbol="MSFT"&_output_format=str
def load_calendar(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/calendar/{symbol}"
    LOG.info(f"Loanding calendar : {symbol}")
    ticket = yf.Ticker(symbol)

    data = ticket.calendar
    for key in data:
        if isinstance(data[key], datetime.date):
            data[key] = str(data[key])
    if 'Earnings Date' in data:
        data['Earnings Date'] = list(
            map(lambda date: str(date), data['Earnings Date']))

    if not 'Ex-Dividend Date' in data:
        data['Ex-Dividend Date'] = "1970-01-01"

    data = {"Date": pd.to_datetime(data['Ex-Dividend Date']),
            "json": [json.dumps(data)]}

    data = pd.DataFrame.from_dict(data)
    data = data.set_index('Date')

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
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

    data['partition'] = data.index
    data['partition'] = pd.Categorical(
        data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)

    data.to_parquet(path,  engine='fastparquet', partition_cols=[
        "partition"], append=append_mode)


# https://localhost:5000/api/call/calendar?symbol="MSFT"&_output_format=str
def calendar(symbol: str = "MSFT") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/calendar/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime(
        '%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_shares?symbol="MSFT"&_output_format=str
def load_shares(symbol: str = "MSFT"):

    path = f"{DATA_PATH}/shares/{symbol}"

    LOG.info(f"Loanding shares : {symbol}")
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

    data = ticker.get_shares_full(start=last_update.strftime('%Y-%m-%d'),
                                  end=None)
    data = data if data is not None else pd.Series()

    if data.shape[0] != 0:
        data = data.to_frame(name="shares")
        data.index.names = ['Date']

        data = data[data.index > last_update.strftime('%Y-%m-%d')]
    LOG.info(f"Last update date: {last_update.strftime('%Y-%m-%d')}")
    LOG.info(f"Number new lines: {str(data.shape[0])}")

    if data.shape[0] != 0:
        data['partition'] = data.index
        data['partition'] = pd.Categorical(
            data['partition'].dt.strftime('%Y-%m'))
        data.reset_index(drop=False, inplace=True)
        data.to_parquet(path,  engine='fastparquet', partition_cols=[
                        "partition"], append=append_mode)


# https://localhost:5000/api/call/shares?symbol="MSFT"&start="2024-11-05"&_output_format=str
def shares(symbol: str = "MSFT",
           start: str = "1900-01-01",
           end: str = "3000-01-01") -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/shares/{symbol}"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)

    data = data[(data.index >= start) & (data.index <= end)]

    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_gold
def load_gold():
    load_stock("GC=F")


# https://localhost:5000/api/call/gold&start="2024-11-05"&_output_format=str
def gold(start: str = "1900-01-01",
         end: str = "3000-01-01") -> pd.core.frame.DataFrame:
    return stock("GC=F")


# https://localhost:5000/api/call/load_fedfunds&_output_format=str
def load_fedfunds():

    start_date = "1954-07-01"
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    path = f"{DATA_PATH}/fedfunds"

    LOG.info(f"Loanding fedfunds")

    config = configparser.ConfigParser()
    config.read('config.ini')

    api_key = config.get("MAIN", "fed.key.api", fallback="")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "FEDFUNDS",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }

    response = requests.get(url, params=params)

    data = response.json()
    data = data.get("observations", [])
    tmp_data = {"Date": [], "value": []}
    for line in data:
        tmp_data["Date"].append(pd.Timestamp(line["date"]))
        tmp_data["value"].append(float(line["value"]))

    data = pd.DataFrame.from_dict(tmp_data)
    data = data.set_index("Date")

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
    data['partition'] = pd.Categorical(data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)
    data.to_parquet(path,  engine='fastparquet', partition_cols=["partition"],
                    append=append_mode)


# https://localhost:5000/api/call/fedfunds?_output_format=str
def fedfunds() -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/fedfunds"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)
    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_unemployment_usa
def load_unemployment_usa():

    start_date = "1954-07-01"
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    path = f"{DATA_PATH}/unemployment_usa"

    LOG.info(f"Loanding fedfunds")

    config = configparser.ConfigParser()
    config.read('config.ini')

    api_key = config.get("MAIN", "fed.key.api", fallback="")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "UNRATE",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }

    response = requests.get(url, params=params)

    data = response.json()
    data = data.get("observations", [])
    tmp_data = {"Date": [], "value": []}
    for line in data:
        tmp_data["Date"].append(pd.Timestamp(line["date"]))
        tmp_data["value"].append(float(line["value"]))

    data = pd.DataFrame.from_dict(tmp_data)
    data = data.set_index("Date")

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
    data['partition'] = pd.Categorical(data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)
    data.to_parquet(path,  engine='fastparquet', partition_cols=["partition"],
                    append=append_mode)


# https://localhost:5000/api/call/unemployment_usa?_output_format=str
def unemployment_usa() -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/unemployment_usa"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)
    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


# https://localhost:5000/api/call/load_inflation_usa
def load_inflation_usa():

    start_date = "1954-07-01"
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    path = f"{DATA_PATH}/inflation_usa"

    LOG.info(f"Loanding fedfunds")

    config = configparser.ConfigParser()
    config.read('config.ini')

    api_key = config.get("MAIN", "fed.key.api", fallback="")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "FPCPITOTLZGUSA",
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }

    response = requests.get(url, params=params)

    data = response.json()
    data = data.get("observations", [])
    tmp_data = {"Date": [], "value": []}
    for line in data:
        tmp_data["Date"].append(pd.Timestamp(line["date"]))
        tmp_data["value"].append(float(line["value"]))

    data = pd.DataFrame.from_dict(tmp_data)
    data = data.set_index("Date")

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
    data['partition'] = pd.Categorical(data['partition'].dt.strftime('%Y-%m'))
    data.reset_index(drop=False, inplace=True)
    data.to_parquet(path,  engine='fastparquet', partition_cols=["partition"],
                    append=append_mode)


# https://localhost:5000/api/call/inflation_usa?_output_format=str
def inflation_usa() -> pd.core.frame.DataFrame:

    path = f"{DATA_PATH}/inflation_usa"
    data = pd.read_parquet(path, engine='fastparquet')
    data.set_index("Date", inplace=True)
    data.reset_index(drop=False, inplace=True)
    data["Date"] = data["Date"].dt.strftime('%Y-%m-%d')
    return data


def loader_SP500():
    for symbol in list(SP500()["Symbol"]):
        load_stock(symbol)
        load_dividends(symbol)
        load_splits(symbol)
        load_income(symbol)
        load_quarterly_income(symbol)
        load_cashflow(symbol)
        load_quarterly_cashflow(symbol)
        load_balance_sheet(symbol)
        load_quarterly_balance_sheet(symbol)
        load_calendar(symbol)
        load_shares(symbol)

#load_gold()
#print(gold())

load_quarterly_income("FTNT")
print(quarterly_income("FTNT"))

exit(0)

loader_SP500()

#exit(0)

load_gold()
# print(gold)


load_fedfunds()
# print(fedfunds())

load_unemployment_usa()
# print(unemployment_usa())

load_inflation_usa()
# print(inflation_usa())


exit()

# -----------------------------------------------------------------------------#
# def scheduler():
#    cpt = 5
#    while True:
#        try:
#            t = 5/cpt
#            cpt -= 1
#            LOG.info("I'm running")
#            print("I'm running")
#            time.sleep(1)
#        except Exception as e:
#            exception = traceback.format_exc()
#            LOG.error(exception)
#            time.sleep(10)
#
#
# threading.Thread(target=scheduler).start()
#
#
# while True:
#    print("exec api")
#    time.sleep(2)

# -----------------------------------------------------------------------------#

msft = yf.Ticker("MSFT")
pprint(msft.earnings_dates)


# TODO: a finir
# Liste des symbol cac40
# les taux de la BCE


# Informations:
# le s&p 500 represent 80% du marché américain
# le états unis produise 25% du PIB mondial
# la capitalisation du cac 40 = 2 460 milliards d'euro en décembre 2023
# la sum des capitalisation du S&P et de 50 000 milliard
