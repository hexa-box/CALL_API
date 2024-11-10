from flask import Flask, request, abort, jsonify
import configparser
import inspect
import json
import traceback
import pickle
import base64


ALLOWED_CALL = set()


def embed(code):

    global ALLOWED_CALL
    old_globals = set(globals().keys())
    exec(code, globals())
    new_globals = set(globals().keys())
    new_call = new_globals-old_globals
    # Filter no callable symbol
    new_call = set(
        filter(lambda symbol: inspect.isfunction(globals()[symbol]), new_call))
    ALLOWED_CALL = ALLOWED_CALL.union(new_call)


embed(open("function.py").read())
embed(open("endpoints.py").read())
embed(code="""
def hello3(name):
    return "Hello 3 "+name
""")


print(f"Authorised call: {str(ALLOWED_CALL)}")

app = Flask(__name__)


# https://localhost:5000/api/call/get_stock?symbol="MSFT"&start="22024-11-05"
# https://localhost:5000/api/call/coucou?a=1
@app.route('/api/call/<fun_name>', methods=['GET'])
def call(fun_name):

    # Check if function call allowed
    if (fun_name not in ALLOWED_CALL):
        return jsonify(error="Bad request",
                       message=f"The {fun_name} "
                       "function does not exist or is not allowed"), 400

    # Get callabel function
    if (fun_name in locals()):
        fun_call = locals()[fun_name]
    else:
        fun_call = globals()[fun_name]

    # Build a kwargs
    tmp_args = []
    needed_arg = list(
        map(lambda arg: arg["name"], get_signature(fun_call)["args"]))

    for arg in request.args:
        if arg in needed_arg:
            tmp_args.append(f'"{arg.strip()}": {request.args.get(arg)}')
    args = "{ "+",".join(tmp_args)+" }"

    # Check if args is a valide json
    try:
        kwargs = json.loads(args)
    except Exception as e:
        return jsonify(
            error="Bad request",
            message=f"The json of the args variable is invalid"), 400

    # Execute called function
    result = None
    exception = None

    try:
        if "_output_format" in request.args:
            output_format = request.args.get("_output_format")
            if output_format == "str":
                result = str(fun_call(**kwargs))
            elif output_format == "binary":
                # pickle.loads(base64.b64decode('xxxx'.encode("ascii")))
                result = base64.b64encode(
                    pickle.dumps(fun_call(**kwargs))).decode("ascii")
            elif output_format == "to_dict(list)":
                result = fun_call(**kwargs).to_dict("list")
            else:
                raise Exception("Unexpected value for _output_format")
        else:
            result = fun_call(**kwargs)
    except Exception as e:
        exception = traceback.format_exc()
        result = None

    return {'result': result, 'exception': exception}


# https://localhost:5000/api/signature/coucou
@app.route('/api/signature/<fun_name>', methods=['GET'])
def signature(fun_name):

    if (fun_name not in ALLOWED_CALL):
        return jsonify(error="Bad request",
                       message=f"The {fun_name} "
                       "function does not exist or is not allowed"), 400

    return get_signature(globals()[fun_name])


def get_signature(fun):

    sing = inspect.signature(fun)
    args = list(
        map(lambda name:
            {'name': name,
             'type': (sing.parameters[name].annotation.__name__
                      if sing.parameters[name].annotation != inspect._empty
                      else None),
             'default': (
                 sing.parameters[name].default
                 if sing.parameters[name].default != inspect._empty
                 else None)
             }, sing.parameters))

    return_type = (sing.return_annotation.__name__
                   if str(sing.return_annotation.__module__) == "builtins"
                   else (sing.return_annotation.__module__ +
                         '.'+sing.return_annotation.__name__))

    return {'args': args,
            "type": return_type}


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
