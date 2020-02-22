from sql import psql
import os

DATABASE_CONFIG = {
    # List vars below
    'host': 'birthday-hackaton.c7qxi2ihw6mz.eu-central-1.rds.amazonaws.com',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'password',
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
WITH
    data AS (SELECT
                 user_uuid
               , unnest(string_to_array(ticker_symbols, '|')) AS ticker_symbol
             FROM public.holdings
                 WHERE user_uuid = '{}')
SELECT DISTINCT *
FROM data;
    """.format(user_id), as_dict=True)
    sql.close()
    for row in ticker_list:
        yield dict(row)


def update_user_holdings(user_id, ticker):
    sql = psql(**DATABASE_CONFIG)
    ticker_list = sql.query("""
    UPDATE public.holdings
    SET
        ticker_symbols = ticker_symbols || '|' || '{}'
    WHERE user_uuid = '{}';
    """.format(ticker, user_id), as_dict=True)
    sql.commit()
    sql.close()
    return '[OK]'


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
