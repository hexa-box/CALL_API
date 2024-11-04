
x = 20

def coucou(a: int)-> str:
    return "l'argument a et de type: "+type(a).__name__ + " et la valeur et "+str(a)

def hello(name):
    return "Hello "+name

def getx(name):
    return "var x = "+str(x)

def badfun(name):
    return not_exist_var 