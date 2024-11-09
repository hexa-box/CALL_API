
import json
import yfinance as yf
import pandas as pd
import datetime
import os
from pprint import pprint

x = 20


DATA_PATH = "data"


def coucou(a: int) -> str:
    return "l'argument a et de type: "+type(a).__name__ + \
        " et la valeur et "+str(a)


def hello(name):
    return "Hello "+name


def getx(name):
    return "var x = "+str(x)


def badfun(name):
    return not_exist_var


#-----------------------------------------------------------------------------#

def is_empty_dir(path):
    if os.path.isdir(path) and os.listdir(path):
        return False
    return True

# https://localhost:5000/api/call/load_stock?symbol="MSFT"
def load_stock(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/stocks/{symbol}"

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
    print(last_update.strftime('%Y-%m-%d'))
    print(hist.shape[0])

    hist['partition'] = hist.index
    hist['partition'] = pd.Categorical(hist['partition'].dt.strftime('%Y-%m'))
    hist.reset_index(drop=False, inplace=True)

    hist.to_parquet(path,  engine='fastparquet', partition_cols=[
                    "partition"], append=append_mode)

    return True


# https://localhost:5000/api/call/get_stock?symbol="MSFT"&start="2024-11-05"
def get_stock(symbol: str = "MSFT",
              start: str = "1900-01-01", 
              end: str = "3000-01-01") -> str:

    path = f"{DATA_PATH}/stocks/{symbol}"
    stock = pd.read_parquet(path, engine='fastparquet')
    stock.set_index("Date", inplace=True)

    stock = stock[(stock.index >= start) & (stock.index <= end)]

    stock.reset_index(drop=False, inplace=True)
    stock["Date"] = stock["Date"].dt.strftime('%Y-%m-%d')
    return stock.to_dict('list')



# https://localhost:5000/api/call/load_div?symbol="MSFT"
def load_div(symbol: str = "MSFT") -> str:
    
    path = f"{DATA_PATH}/dividends/{symbol}"
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
    print(last_update)
    print(df_div.shape[0])

    df_div['partition'] = df_div.index
    df_div['partition'] = pd.Categorical(
        df_div['partition'].dt.strftime('%Y-%m'))
    df_div.reset_index(drop=False, inplace=True)

    df_div.to_parquet(path,  engine='fastparquet', partition_cols=[
                    "partition"], append=append_mode)
    
    return True
    

# https://localhost:5000/api/call/get_div?symbol="MSFT"
def get_div(symbol: str = "MSFT") -> str:

    path = f"{DATA_PATH}/dividends/{symbol}"
    df_div = pd.read_parquet(path, engine='fastparquet')
    df_div.set_index("Date", inplace=True)

    df_div.reset_index(drop=False, inplace=True)
    df_div["Date"] = df_div["Date"].dt.strftime('%Y-%m-%d')
    return df_div.to_dict('list')



#load_stock("GOOG")
# data = get_stock("GOOG", start="2024-11-05")
# print(data.keys())
# print(json.dumps(data, indent = 4))

#load_div()
#pprint(get_div())


# CA
# resultat net 
# fiscalité et taxe 
# resultat d'exploitation 
# dette
# distripution des dividendes 
# investisement 
# cacheflow 
# 

# le s&p 500 represent 80% du marché américain
# le états unis produise 25% du PIB mondial 
# la capitalisation du cac 40 = 2 460 milliards d'euro en décembre 2023
# la sum des capitalisation du S&P et de 50 000 milliard


# base64.b64encode(pickle.dumps(["coucou", 6])).decode("ascii")
# pickle.loads(base64.b64decode('gASVEAAAAAAAAABdlCiMBmNvdWNvdZRLBmUu'.encode("ascii")))