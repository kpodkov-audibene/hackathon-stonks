from sql import psql
import os

DATABASE_CONFIG = {
    # List vars below
    'host': os.getenv('RD_OPTION_DB_HOST'),
    'port': os.getenv('RD_OPTION_DB_PORT'),
    'database': os.getenv('RD_OPTION_DB_NAME'),
    'user': os.getenv('RD_OPTION_DB_USER'),
    'password': os.getenv('RD_OPTION_DB_PASS'),
}


def get_world_ticker_list():
    sql = psql(**DATABASE_CONFIG)
    ticker_list = sql.query("""
    SELECT iso3, name, ticker_symbol
    FROM public.world
    WHERE ticker_symbol != '';
    """, as_dict=True)
    sql.close()
    for row in ticker_list:
        yield dict(row)


def get_user_holdings(user_id):
    sql = psql(**DATABASE_CONFIG)
    ticker_list = sql.query("""
    SELECT
        user_uuid
      , unnest(string_to_array(ticker_symbols, '|')) AS ticker_symbol
    FROM public.holdings
    WHERE user_uuid = '{}';
    """.format(user_id), as_dict=True)
    sql.close()
    for row in ticker_list:
        yield dict(row)


def get_user_list():
    sql = psql(**DATABASE_CONFIG)
    result = sql.query("""
    SELECT
        uuid
      , first_name
      , last_name
      , email
      , country
      , password
    FROM public.user;
    """, as_dict=True)
    sql.close()

    if not result:
        raise IndexError

    for row in result:
        yield dict(row)
