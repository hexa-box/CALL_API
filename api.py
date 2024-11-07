from flask import Flask, request, abort, jsonify
import configparser
import inspect
import json

def embed(code):
    
    old_globals = set(globals().keys())
    exec(code, globals())
    new_globals = set(globals().keys())
    allowed_call = new_globals-old_globals-set(["__warningregistry__","old_globals"])
    # Filter no callable symbol
    allowed_call = set(filter(lambda symbol: callable(globals()[symbol]), allowed_call))

    return allowed_call


allowed_call = embed(open("function.py").read())
allowed_call2 = embed(code = """
def hello3(name):
    return "Hello 3 "+name
""")
allowed_call = allowed_call.union(allowed_call2)

print(allowed_call)

app = Flask(__name__)


# https://localhost:5000/api/call/get_stock?symbol="MSFT"&start="22024-11-05"
# https://localhost:5000/api/call/coucou?a=1
@app.route('/api/call/<fun_name>', methods=['GET'])
def call(fun_name):
  
    # Build a kwargs
    tmp_args = []
    for arg in request.args:
        tmp_args.append(f'"{arg.strip()}": {request.args.get(arg)}')
    args = "{ "+",".join(tmp_args)+" }"

    # Check if function call allowed
    if(fun_name not in allowed_call):
        return jsonify(error="Bad request", message=f"The {fun_name} function does not exist or is not allowed"), 400
    
    # Check if args is a valide json 
    try:
        kwargs = json.loads(args)
    except Exception as e:
        return jsonify(error="Bad request", message=f"The json of the args variable is invalid"), 400
     
    # Execute called function 
    if(fun_name in locals()):
        fun_call = locals()[fun_name]
    else:
        fun_call = globals()[fun_name]

    result = None
    exception = None
    try:
        result =  str(fun_call(**kwargs))
    except Exception as e:
        exception = str(e)

    return {'result': result, 'exception': exception}


# https://localhost:5000/api/signature/coucou
@app.route('/api/signature/<fun_name>', methods=['GET'])
def signature(fun_name):
   
    if(fun_name not in allowed_call):
        return jsonify(error="Bad request", message=f"The {fun_name} function does not exist or is not allowed"), 400
    
    sing = inspect.signature(globals()[fun_name])
    args = list(map(lambda name: {'name': name,
                                  'type': sing.parameters[name].annotation.__name__ if sing.parameters[name].annotation != inspect._empty else None,
                                  'default': sing.parameters[name].default if sing.parameters[name].default != inspect._empty else None
                                  } ,sing.parameters))
    return {'args' : args, "type": sing.return_annotation.__name__}
    
# La commande pour la génération du certificat :
# openssl req -x509 -nodes -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365

if __name__ == '__main__':
     
    config = configparser.ConfigParser()
    config.read('config.ini')

    host = config.get("MAIN", "api.host", fallback="0.0.0.0")
    port = config.get("MAIN", "api.port", fallback="5000")
    is_ssl = config.getboolean("MAIN", "ssl", fallback=False)
    ssl_cert = config.get("MAIN", "ssl.cert", fallback=None)
    ssl_key = config.get("MAIN", "ssl.key", fallback=None)

    if is_ssl:
        app.run(ssl_context=(ssl_cert, ssl_key), 
                debug=True, host=host, port=port)
    else: 
        app.run(debug=True, host=host, port=port)
