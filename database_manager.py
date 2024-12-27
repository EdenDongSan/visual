import mysql.connector
from mysql.connector import pooling
import logging
import os
from typing import List, Dict, Optional, Union
from dataclasses import dataclass
from datetime import datetime
import time
import threading

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.initialized = False
            return cls._instance
    
    def __init__(self):
        with self._lock:
            if self.initialized:
                return
                
            self.pool = None
            self.setup_database()
            self.initialized = True
    
    def setup_database(self):
        """Database connection pool and table setup"""
        try:
            dbconfig = {
                "host": os.getenv('MYSQL_HOST'),
                "user": os.getenv('MYSQL_USER'),
                "password": os.getenv('MYSQL_PASSWORD'),
                "database": os.getenv('MYSQL_DATABASE'),
                "pool_name": "mypool",
                "pool_size": 5,
                "pool_reset_session": True,
                "connect_timeout": 10
            }
            
            self.pool = mysql.connector.pooling.MySQLConnectionPool(**dbconfig)
            
            self._setup_tables()
            logger.info("Database setup completed successfully")
            
        except Exception as e:
            logger.error(f"Database setup error: {e}")
            raise
    
    def _setup_tables(self):
        """Set up database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Create tables with optimized indexes
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS kline_1m (
                        timestamp BIGINT PRIMARY KEY,
                        open FLOAT,
                        high FLOAT,
                        low FLOAT,
                        close FLOAT,
                        volume FLOAT,
                        quote_volume FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created_at (created_at)
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS long_short_ratio (
                        timestamp BIGINT PRIMARY KEY,
                        long_ratio FLOAT,
                        short_ratio FLOAT,
                        long_short_ratio FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created_at (created_at)
                    )
                """)
                
                # Add data retention procedure
                cursor.execute("""
                    CREATE EVENT IF NOT EXISTS cleanup_old_data
                    ON SCHEDULE EVERY 1 DAY
                    DO BEGIN
                        DELETE FROM kline_1m 
                        WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
                        DELETE FROM long_short_ratio 
                        WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
                    END
                """)
                
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Error setting up tables: {e}")
                raise
            finally:
                cursor.close()
    
    def get_connection(self):
        """Get connection from pool with retry mechanism"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                return self.pool.get_connection()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to get database connection after {max_retries} attempts")
                    raise
                logger.warning(f"Failed to get connection (attempt {attempt + 1}): {e}")
                time.sleep(retry_delay * (2 ** attempt))
    
    def execute_with_retry(self, operation: callable, *args, **kwargs):
        """Execute database operation with retry mechanism"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        result = operation(cursor, *args, **kwargs)
                        conn.commit()
                        return result
                    except Exception as e:
                        conn.rollback()
                        raise
                    finally:
                        cursor.close()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Operation failed after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"Operation failed (attempt {attempt + 1}): {e}")
                time.sleep(retry_delay * (2 ** attempt))
    
    def store_candle(self, candle: Candle):
        """Store candle data with retry mechanism"""
        def _store_candle(cursor, candle):
            cursor.execute("""
                INSERT INTO kline_1m 
                (timestamp, open, high, low, close, volume, quote_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume),
                quote_volume=VALUES(quote_volume)
            """, (
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume
            ))
        
        try:
            self.execute_with_retry(_store_candle, candle)
        except Exception as e:
            logger.error(f"Error storing candle data: {e}")
            raise
    
    def store_long_short_ratio(self, timestamp: int, long_ratio: float, 
                             short_ratio: float, long_short_ratio: float):
        """Store long/short ratio data with retry mechanism"""
        def _store_ratio(cursor, *args):
            cursor.execute("""
                INSERT INTO long_short_ratio 
                (timestamp, long_ratio, short_ratio, long_short_ratio)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                long_ratio=VALUES(long_ratio),
                short_ratio=VALUES(short_ratio),
                long_short_ratio=VALUES(long_short_ratio)
            """, args)
        
        try:
            self.execute_with_retry(_store_ratio, timestamp, long_ratio, 
                                  short_ratio, long_short_ratio)
        except Exception as e:
            logger.error(f"Error storing long/short ratio data: {e}")
            raise
    
    def get_recent_candles(self, limit: int = 200) -> List[Candle]:
        """Get recent candle data with retry mechanism"""
        def _get_candles(cursor, limit):
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, quote_volume
                FROM kline_1m
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            return [
                Candle(
                    timestamp=row[0],
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    quote_volume=float(row[6]) if row[6] else None
                )
                for row in rows
            ][::-1]  # Reverse for chronological order
        
        try:
            return self.execute_with_retry(_get_candles, limit)
        except Exception as e:
            logger.error(f"Error fetching recent candles: {e}")
            return []
    
    def get_recent_ratio(self, limit: int = 200) -> List[Dict]:
        """Get recent long/short ratio data with retry mechanism"""
        def _get_ratios(cursor, limit):
            cursor.execute("""
                SELECT timestamp, long_ratio, short_ratio, long_short_ratio
                FROM long_short_ratio
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            return [
                {
                    'timestamp': row[0],
                    'long_ratio': float(row[1]),
                    'short_ratio': float(row[2]),
                    'long_short_ratio': float(row[3])
                }
                for row in rows
            ][::-1]  # Reverse for chronological order
        
        try:
            return self.execute_with_retry(_get_ratios, limit)
        except Exception as e:
            logger.error(f"Error fetching recent ratios: {e}")
            return []
    
    def cleanup_old_data(self, days: int = 30):
        """Manual cleanup of old data"""
        def _cleanup(cursor, days):
            cursor.execute("""
                DELETE FROM kline_1m 
                WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (days,))
            
            cursor.execute("""
                DELETE FROM long_short_ratio 
                WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (days,))
        
        try:
            self.execute_with_retry(_cleanup, days)
            logger.info(f"Successfully cleaned up data older than {days} days")
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            raise