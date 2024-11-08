
import yfinance as yf
import pandas as pd
import datetime

x = 20

DATA_PATH = "data/"

def coucou(a: int)-> str:
    return "l'argument a et de type: "+type(a).__name__ + " et la valeur et "+str(a)

def hello(name):
    return "Hello "+name

def getx(name):
    return "var x = "+str(x)

def badfun(name):
    return not_exist_var 


# https://localhost:5000/api/call/load_stock?symbol="MSFT"
def load_stock(symbol: str = "MSFT")-> str:

    path = f"{DATA_PATH}/stocks/{symbol}"
    
    ticker = yf.Ticker(symbol)

    # get all stock info
    #ticker.info

    try:
        last_update = pd.read_parquet(path, engine='fastparquet', columns=["Date"])["Date"].max()   
        append_mode = True
    except FileNotFoundError :
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False

    hist = ticker.history(start=last_update.strftime('%Y-%m-%d'))
    hist = hist[hist.index > last_update.strftime('%Y-%m-%d')]
    
    hist['partition'] = hist.index
    hist['partition'] = pd.Categorical(hist['partition'].dt.strftime('%Y-%m'))
    hist.reset_index(drop=False, inplace=True)
    
    hist.to_parquet(path,  engine='fastparquet', partition_cols=["partition"], append=append_mode)
   
    return True


# https://localhost:5000/api/call/get_stock?symbol="MSFT"&start="2024-11-05"
def get_stock(symbol: str = "MSFT", 
            start: str = "1900-01-01", end: str = "3000-01-01")-> str:

    path = f"{DATA_PATH}/stocks/{symbol}"
    stock = pd.read_parquet(path, engine='fastparquet')
    stock.set_index("Date", inplace=True)

    stock = stock[(stock.index >= start) & (stock.index <= end)]

    return stock.to_dict()

#load_stock("GOOG")
#print(get_stock("GOOG", start="2024-11-05"))
