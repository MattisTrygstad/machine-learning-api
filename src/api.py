import flask
from flask_cors import CORS
import json
from flask import jsonify
from flask.helpers import make_response
from flask import request
from fetcher import Fetcher
from flask import Response

app = flask.Flask(__name__)
app.config["DEBUG"] = True
CORS(app)


@app.route('/', methods=['GET'])
def home():
    x = {
        "name": "John",
        "age": 30,
        "city": "New York"
    }
    return make_response(jsonify(x), 200)


@app.route('/<date>', methods=['GET'])
def date(date):
    df = Fetcher().fetch_data()
    print(df)
    return Response(df.to_json(orient="records"), mimetype='application/json')


app.run(host='192.168.0.3')
