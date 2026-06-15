from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from market_db import db_controllers 
import logging
from datetime import datetime
from typing import List, Dict, Any

# Setting logging module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Candle(BaseModel):
    ts: int
    open: float
    close: float
    high: float
    low: float
    volume: int

# API Object for the Market Database
class MktDBAPI():
    def __init__(self):
        self.app = FastAPI(title="Market DB API")

        self.origins = [
            "http://192.168.0.122:8000"
            ]
        
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self.current_model = {
            "row": "*",
            "table": "candle_meta"
        }

        self.register_routes()
        

    def register_routes(self):
        """Registering routes for api object"""
        @self.app.get("/ping")
        def ping():
            return {"status": "ok"}
        
# Route for retrieving data from database
        @self.app.get("/get_data/{ticker}/{tf}")
        def get_table_data(ticker: str, tf: str, limit: int=1000):
            try:
                conn = db_controllers.establish_connection()
                logger.info(ticker.lower())
                data = db_controllers.get_data(conn, self.current_model["row"], ticker.lower(), tf)
                logger.info("Data found, returning now!")
                formatted = self.format_candles(data)
                return_data = formatted[-limit:]
                logger.info(f"Returning {len(return_data)} candles out of {len(formatted)}")
                return return_data
            except Exception as e:
                print(e)
                return None
# Route for retrieving the last known date entered into the table
        @self.app.get("/get_last_entry/{ticker}/{tf}")
        def get_last_entry(ticker: str, tf: str):
            conn = db_controllers.establish_connection()
            entry = db_controllers.get_last_date(conn, ticker, tf)
            logger.info(entry)
            return entry
        
# Route for adding data to the database by candle
        @self.app.post("/append_database/{ticker}/{tf}")
        def append_database(candle: Candle, ticker: str, tf: str):
            conn = db_controllers.establish_connection()
            try:
                db_controllers.insert_data(conn, ticker, tf, dict(candle))
                logger.info(candle)
                return {"status": "success"}

            except psycopg2.errors.UndefinedTable:
                conn.rollback()

                try:
                    logger.info("Checking for table existence")
                    db_controllers.establish_table_existence(conn, ticker, tf)
                    logger.info("Inserting data... again")
                    db_controllers.insert_data(conn, ticker, tf, dict(candle))
                    logger.info(candle)
                    return {"status": "success"}

                except Exception as e:
                    logger.error(f"Insert failed: {e}")
                    logger.error(candle)
                    return {"status": "error"}

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Unexpected DB error: {e}")
                    return {"status": "error"}

            finally:
                conn.close()
    
        @self.app.post("/append_database_batch/{ticker}/{tf}")
        def append_database_batch(
            ticker: str,
            tf: str,
            candles: List[Dict[str, Any]]
            ):
            """Insert multiple candles at once."""
            conn = db_controllers.establish_connection()
            try:
                db_controllers.insert_data_batch(conn, ticker, tf, candles)
                return {"status": "success", "count": len(candles)}
            except psycopg2.errors.UndefinedTable:
                conn.rollback()

                try:
                    logger.info("Checking for table existence")
                    db_controllers.establish_table_existence(conn, ticker, tf)
                    logger.info("Inserting data... again")
                    db_controllers.insert_data_batch(conn, ticker, tf, candles)
                    return {"status": "success"}
                
                except Exception as e:
                    logger.error(f"Insert failed: {e}")
                    return {"status": "error"}

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Unexpected DB error: {e}")
                    return {"status": "error"}


            finally:
                conn.close()

    def format_candles(self, rows):
        formatted = []

        for r in rows:

            timestamp = int(r[0].timestamp())

            candle = {
                "timestamp": timestamp,
                "datetime": r[0].strftime("%Y-%m-%d %H:%M:%S"),
                "open": r[1],
                "high": r[3],
                "low": r[4],
                "close": r[2],
                "volume": r[5]
            }

            formatted.append(candle)

        return formatted