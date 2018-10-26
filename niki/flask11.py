import math
import threading

from flask import Flask, request
from flask.json import jsonify

app = Flask(__name__)


@app.route('/f/')
def hello_world():
    return 'Hello, World!'


if __name__ == '__main__':
    app.run()
