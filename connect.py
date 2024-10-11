import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#########################################################################################################################
## Before starting, please have Postgresql install, please follow this instruction to install and setup timescale DB   ##
## https://youtu.be/A73bZISslQQ?feature=shared                                                                         ##
## full playlist: https://youtube.com/playlist?list=PLsceB9ac9MHScvW5NBuCaYafW87hP-Gi2&feature=shared                  ##
#########################################################################################################################

# Create connection
conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="password",
                        port=5432)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

#########################
##### create tables #####
#########################

create_company_table = """CREATE TABLE company(symbol text PRIMARY KEY NOT NULL,name text NOT NULL);"""
cursor.execute(create_company_table)
create_stock_table = """CREATE TABLE stocks_real_time(
time TIMESTAMPTZ NOT NULL,
symbol text NOT NULL,
price float NOT NULL,
day_volume INT NULL);"""
cursor.execute(create_stock_table)
create_index_symbol = """CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);"""
cursor.execute(create_index_symbol)
create_hybertable = """SELECT create_hypertable('stocks_real_time','time');"""
cursor.execute(create_hybertable)
conn.commit()

#########################
#####   querying    #####
#########################

print("\nSelect all stock data from the last 3 day")
select_stocks_last_day = """SELECT * FROM stocks_real_time srt WHERE time > now() - INTERVAL '3 days';"""
cursor.execute(select_stocks_last_day)
print(pd.DataFrame(cursor.fetchall()))

print("\nSelect top 10 stocks traded by price")
top_ten_price = """SELECT * FROM stocks_real_time srt ORDER BY time DESC, price DESC LIMIT 10;"""
cursor.execute(top_ten_price)
print(pd.DataFrame(cursor.fetchall()))

print("\nGet the first and last trading value of each company with first() and last()")
# first, get the value of the earliest
# last, get the value of the latest
first_last = """SELECT symbol, first(price,time), last(price,time)
FROM stocks_real_time srt
WHERE time > now() - INTERVAL '3 days' GROUP BY symbol;"""
cursor.execute(first_last)
print(pd.DataFrame(cursor.fetchall()))

print("\nAggregate by an arbitrary length of time using time_bucket") # really fast compare to normal query
time_bucket = """SELECT 
time_bucket('1 day',time) AS bucket,
symbol,
avg(price)
FROM stocks_real_time srt
WHERE time > now() - INTERVAL '1 week'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;"""
cursor.execute(time_bucket)
print(pd.DataFrame(cursor.fetchall()))

print("\nContinuous Aggregates")
time_bucket_candle = """SELECT 
time_bucket('1 day',time) AS day,
symbol,
max(price) AS high,
first(price,time) AS open,
last(price,time) AS close,
min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol
ORDER BY day DESC, symbol;"""
cursor.execute(time_bucket_candle)
print(pd.DataFrame(cursor.fetchall()))

material_view_ca = """
CREATE MATERIALIZED VIEW stock_candlestick_daily
WITH(timescaledb.continuous) AS
SELECT 
    time_bucket('1 day',time) AS day,
    symbol,
    max(price) AS high,
    first(price,time) AS open,
    last(price,time) AS close,
    min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol;
"""
cursor.execute(material_view_ca)
conn.commit()

material_view_ca_query = """
SELECT * FROM stock_candlestick_daily
ORDER BY day DESC, symbol;
"""
cursor.execute(material_view_ca_query)
print(pd.DataFrame(cursor.fetchall())) # faster than the time_bucket_candle
