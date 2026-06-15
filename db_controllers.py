import os
import re
import json
import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
from psycopg2.extras import execute_values
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Controllers for interacting with the Market Database

def establish_connection():
    conn = psycopg2.connect(
    dbname="Market-Data",
    user=os.environ["psqluser"],
    password=os.environ["psqlpass"],
    host="localhost",
    port=5432
    )
    return conn   

# To clean entries and make them SQL compatable
def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower())

# Creating new tables (Must match rows in meta table)
def create_table(conn, symbol: str, timeframe: str, table_name=None):
    symbol_s = sanitize(symbol)
    timeframe_s = sanitize(timeframe)
    table_name = f"{symbol_s}_{timeframe_s}"

    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
    ts TIMESTAMPTZ PRIMARY KEY,
    open NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(20, 4)
    );
    """).format(sql.Identifier(table_name))

    with conn.cursor() as cur:
        print("Adding table")
        cur.execute(query)
        print(f"Table added, appending meta table: {table_name}")

    insert_to_meta_table(conn, symbol, timeframe)
    return table_name 

# Adding rows to meta table (Must match tables in database)
def insert_to_meta_table(conn, symbol, timeframe):
    symbol_s = sanitize(symbol)
    timeframe_s = sanitize(timeframe)
    table_name = f"{symbol_s}_{timeframe_s}"

    meta_sql = """
    INSERT INTO candle_meta (symbol, timeframe, table_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (table_name) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(meta_sql, (symbol_s, timeframe_s, table_name))
        logger.info("Meta table appended")
    return table_name

# Ensure that the table in question exists and posesses a meta table counterpart
def establish_table_existence(conn, symbol, timeframe):
    symbol_s = sanitize(symbol)
    timeframe_s = sanitize(timeframe)
    table_name = f"{symbol_s}_{timeframe_s}"

    meta_query = """
    SELECT table_name 
    FROM candle_meta
    WHERE table_name = %s;
    """
    reg_query = """SELECT to_regclass(%s);
    """

    with conn.cursor() as cur:
        cur.execute(meta_query, (table_name,))
        result = cur.fetchone()
        table_name = f"public.{table_name}"
        cur.execute(reg_query, (table_name,))
        result2 = cur.fetchone()
        meta_exists = result is not None
        reg_exists = result2[0] is not None
    # Ensure table exists
    if not reg_exists:
        logger.info("Creating candle table...")
        create_table(conn, symbol, timeframe)
    else:
        logger.info("Found table in database")

    # Then ensure meta entry exists
    if not meta_exists:
        logger.info("Adding to meta table...")
        insert_to_meta_table(conn, symbol, timeframe)
    else:
        logger.info("Found table in meta table")
    conn.commit()

# Acquire the last date entered into the database
def get_last_date(conn, symbol, timeframe):
    symbol_s = sanitize(symbol)
    timeframe_s = sanitize(timeframe)
    table_name = f"{symbol_s}_{timeframe_s}"

    query = sql.SQL("""
    SELECT MAX(ts)
    FROM {};""").format(sql.Identifier(table_name))
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                result = cur.fetchone()
            except psycopg2.errors.UndefinedTable:
                logger.info("Table not found! Establishing existence of table and retrying")
                establish_table_existence(conn, symbol_s, timeframe_s)
                try:
                    cur.execute(query)
                    result = cur.fetchone()
                except Exception as e:
                    return logger.error(f"Failed once again! Aborting data collection process due to: {e}")
        logger.info(type(result))
        return result
    except psycopg2.errors.InFailedSqlTransaction:
        logger.info("Transaction failed! Rolling back transaction")
        conn.rollback()

# Basic query functions
# GET
def get_data(conn, data, ticker, tf):
    table = f"{ticker}_{tf}"
    query = f"""SELECT {data} FROM {table} ORDER BY ts ASC"""
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return rows
# POST
def insert_data(conn, ticker, tf, data):
    table_name = f"{ticker}_{tf}"
    query = f"""INSERT INTO {table_name} (ts, open, close, high, low, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (ts) DO NOTHING;
    """
    with conn.cursor() as cur:
        ts = datetime.fromtimestamp(data["ts"], tz=timezone.utc)
        logger.info(f"Inserting into: {table_name}")
        cur.execute(query, (
            ts,
            data["open"],
            data["close"],
            data["high"],
            data["low"],
            data.get("volume")
        ))
        conn.commit()
def insert_data_batch(conn, ticker: str, tf: str, candles: List[Dict[str, Any]]):
    table_name = f"{sanitize(ticker)}_{sanitize(tf)}"
    data_tuples = []

    for candle in candles:
        ts = datetime.fromtimestamp(candle["ts"], tz=timezone.utc)
        data_tuples.append((
            ts,
            candle["open"],
            candle["close"],
            candle["high"],
            candle["low"],
            candle.get("volume")
        ))
    
    query = f"""
    INSERT INTO {table_name} (ts, open, close, high, low, volume)
    VALUES %s
    ON CONFLICT (ts) DO NOTHING;
    """
    
    with conn.cursor() as cur:
        logger.info(f"Batch inserting {len(data_tuples)} candles into {table_name}")
        execute_values(cur, query, data_tuples)
    
    conn.commit()
    logger.info(f"Successfully inserted {len(data_tuples)} candles")


# DELETE
def delete_table(conn, table):
    query = f"""
        DROP TABLE IF EXISTS {table};
    """
    meta_query = f"""DELETE FROM candle_meta WHERE table_name = %s;"""
    with conn.cursor() as cur:
        logger.info(f"Dropping Table: {table}")
        cur.execute(query)
        cur.execute(meta_query, (table,))
    conn.commit()

def delete_from_table(conn, table, tf):
    query = f"""
        DELETE FROM candles_{table}_{tf};
    """
    with conn.cursor() as cur:
        logger.info(f"Deleting all data from candles_{table}_{tf}")
        cur.execute(query)
    conn.commit()

def manual_rollback():
    try:
        conn = establish_connection()
        try:
            conn.rollback()
            logger.info("Successfully rolled back!")
        except psycopg2.error as e:
            logger.error(f"Rollback Failed, encountered error: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to establish connection: {e}")



