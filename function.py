
import yfinance as yf
import pandas as pd
import datetime

x = 20

def coucou(a: int)-> str:
    return "l'argument a et de type: "+type(a).__name__ + " et la valeur et "+str(a)

def hello(name):
    return "Hello "+name

def getx(name):
    return "var x = "+str(x)

def badfun(name):
    return not_exist_var 

# https://localhost:5000/api/call/getHistory?args={"symbol":"MSFT"}
def getHistory(symbol: str = "MSFT", period="1mo")-> str:

    path = f"data/stocks/{symbol}"
    
    ticker = yf.Ticker(symbol)

    # get all stock info
    #ticker.info

    hist = ticker.history(period=period)
  
    try:
        last_update = pd.read_parquet(path, columns=["Date"]).index.max()   
        append_mode = True
    except FileNotFoundError :
        last_update = pd.Timestamp(datetime.datetime(1970, 1, 1, 0, 0))
        append_mode = False
    
    print(f"Last update = {last_update}")

    hist = hist[hist.index > last_update.strftime('%Y-%m-%d')]

    hist['partition'] = hist.index
    hist['partition'] = pd.Categorical(hist['partition'].dt.strftime('%Y-%m'))

    hist.to_parquet(path,  engine='fastparquet', partition_cols=["partition"], append=append_mode)

    df2 = pd.read_parquet(path)
   
    return str(df2)

getHistory("MSFT")