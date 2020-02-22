from flask import Flask
from flask_jwt import JWT, jwt_required, current_identity
from werkzeug.security import safe_str_cmp
import model
import http_client
import json

http = http_client.Client()


class User(object):
    def __init__(self, id, first_name, last_name, email, country, password):
        self.id = id
        self.username = email
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.country = country
        self.password = password

    def __str__(self):
        return json.dumps({'id': self.id,
                           'email': self.email,
                           'first_name': self.first_name,
                           'last_name': self.last_name,
                           'country': self.country})


users = []
for user in model.get_user_list():
    users.append(
        User(user['uuid'], user['first_name'], user['last_name'], user['email'], user['country'], user['password']))

username_table = {u.username: u for u in users}
userid_table = {u.id: u for u in users}


def authenticate(username, password):
    user = username_table.get(username, None)
    if user and safe_str_cmp(user.password.encode('utf-8'), password.encode('utf-8')):
        return user


def identity(payload):
    user_id = payload['identity']
    return userid_table.get(user_id, None)


app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = 'super-secret'

jwt = JWT(app, authenticate, identity)


@app.route('/api/get_global_status')
def get_global_status():
    response = []
    for global_ticker in model.get_world_ticker_list():
        result = http.get(f"https://financialmodelingprep.com/api/v3/quote/{global_ticker['ticker_symbol']}").json()
        for financial_index in result:
            response.append({global_ticker['iso3']: {'changesPercentage': financial_index['changesPercentage']
                , 'price': financial_index['price'], 'country': global_ticker['name']}})
    return str(response)


@app.route('/api/get_user_holdings')
@jwt_required()
def get_user_holdings():
    response = []
    for user_ticker in model.get_user_holdings(current_identity.id):
        result = http.get(f"https://financialmodelingprep.com/api/v3/quote/{user_ticker['ticker_symbol']}").json()
        for financial_index in result:
            response.append({'ticker_symbol': user_ticker['ticker_symbol']
                                , 'changesPercentage': financial_index['changesPercentage']
                                , 'price': financial_index['price']})
    return str(response)


@app.route('/protected')
@jwt_required()
def get_portfolio():
    current_identity['user_id']


@app.route('/')
@jwt_required()
def index():
    return '%s' % current_identity


if __name__ == '__main__':
    app.run()