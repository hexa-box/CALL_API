
import yfinance as yf

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
def getHistory(symbol: str)-> str:
    
    ticker = yf.Ticker(symbol)

    # get all stock info
    #ticker.info

    # get historical market data
    hist = ticker.history(period="1mo")

    hist.index = hist.index.tz_convert(None)
    #hist.to_parquet("data/table",  engine='fastparquet', partition_cols=["Date"])
    hist.to_parquet("data/table",  engine='fastparquet', partition_cols=["Date"], append=True)
    return str(hist)

print(getHistory("MSFT"))