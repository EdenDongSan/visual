# database_manager.py
import mysql.connector
import logging
import os
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime

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
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not DatabaseManager._initialized:
            try:
                self.db = self._setup_database()
                DatabaseManager._initialized = True
            except Exception as e:
                logger.error(f"Failed to initialize DatabaseManager: {e}")
                raise
    
    def _setup_database(self):
        """데이터베이스 연결 및 테이블 설정"""
        try:
            db = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD')
            )
            
            cursor = db.cursor()
            db_name = os.getenv('MYSQL_DATABASE')
            
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")
            
            # 기존 캔들 테이블
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS kline_1m (
                timestamp BIGINT PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                quote_volume FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 시장 지표 테이블 추가
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_indicators (
                timestamp BIGINT PRIMARY KEY,
                open_interest FLOAT,
                long_ratio FLOAT,
                short_ratio FLOAT,
                long_short_ratio FLOAT,
                oi_slope FLOAT,
                ls_ratio_slope FLOAT,
                ls_ratio_acceleration FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 거래 기록 테이블 추가
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                timestamp BIGINT,
                symbol VARCHAR(20),
                side VARCHAR(10),
                size FLOAT,
                entry_price FLOAT,
                exit_price FLOAT,
                pnl FLOAT,
                pnl_percentage FLOAT,
                leverage INT,
                trade_type VARCHAR(20),
                entry_type VARCHAR(20),
                exit_reason VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            db.commit()
            cursor.close()
            return db
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def reconnect(self):
        """DB 재연결"""
        try:
            if not self.db.is_connected():
                self.db = self._setup_database()
        except Exception as e:
            logger.error(f"Database reconnection error: {e}")
            raise

    def store_candle(self, candle: Candle):
        """단일 캔들 데이터 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                INSERT INTO kline_1m 
                (timestamp, open, high, low, close, volume, quote_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
            """, (
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.quote_volume
            ))
            
            self.db.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing candle data: {e}")
            self.db.rollback()

    def get_recent_candles(self, limit: int = 200) -> List[Candle]:
        """최근 캔들 데이터 조회"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, quote_volume
                FROM kline_1m
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            
            rows = cursor.fetchall()
            cursor.close()
            
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
            ]
            
        except Exception as e:
            logger.error(f"Error fetching recent candles: {e}")
            return []

    def store_initial_candles(self, candles: List[Dict]):
        """초기 캔들 데이터 일괄 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            for candle_data in candles:
                cursor.execute("""
                    INSERT INTO kline_1m 
                    (timestamp, open, high, low, close, volume, quote_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open=%s, high=%s, low=%s, close=%s, volume=%s, quote_volume=%s
                """, (
                    int(candle_data[0]),     # timestamp
                    float(candle_data[1]),   # open
                    float(candle_data[2]),   # high
                    float(candle_data[3]),   # low
                    float(candle_data[4]),   # close
                    float(candle_data[5]),   # volume
                    float(candle_data[6]),   # quote_volume
                    float(candle_data[1]),   # open
                    float(candle_data[2]),   # high
                    float(candle_data[3]),   # low
                    float(candle_data[4]),   # close
                    float(candle_data[5]),   # volume
                    float(candle_data[6])    # quote_volume
                ))
            
            self.db.commit()
            cursor.close()
            logger.info(f"Successfully stored {len(candles)} initial candles")
            
        except Exception as e:
            logger.error(f"Error storing initial candles: {e}")
            self.db.rollback()


    def store_market_indicators(self, timestamp: int, indicators: dict):
        """시장 지표 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                INSERT INTO market_indicators 
                (timestamp, open_interest, long_ratio, short_ratio, long_short_ratio,
                oi_slope, ls_ratio_slope, ls_ratio_acceleration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open_interest=%s, long_ratio=%s, short_ratio=%s, long_short_ratio=%s,
                oi_slope=%s, ls_ratio_slope=%s, ls_ratio_acceleration=%s
            """, (
                timestamp,
                indicators.get('open_interest', 0),
                indicators.get('long_ratio', 0),
                indicators.get('short_ratio', 0),
                indicators.get('long_short_ratio', 0),
                indicators.get('oi_slope', 0),
                indicators.get('ls_ratio_slope', 0),
                indicators.get('ls_ratio_acceleration', 0),
                indicators.get('open_interest', 0),
                indicators.get('long_ratio', 0),
                indicators.get('short_ratio', 0),
                indicators.get('long_short_ratio', 0),
                indicators.get('oi_slope', 0),
                indicators.get('ls_ratio_slope', 0),
                indicators.get('ls_ratio_acceleration', 0)
            ))
            
            self.db.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing market indicators: {e}")
            self.db.rollback()

    def store_trade(self, trade_data: dict):
        """거래 기록 저장"""
        try:
            self.reconnect()
            cursor = self.db.cursor()
            
            cursor.execute("""
                INSERT INTO trade_history 
                (timestamp, symbol, side, size, entry_price, exit_price,
                pnl, pnl_percentage, leverage, trade_type, entry_type, exit_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                trade_data['timestamp'],
                trade_data['symbol'],
                trade_data['side'],
                trade_data['size'],
                trade_data['entry_price'],
                trade_data['exit_price'],
                trade_data['pnl'],
                trade_data['pnl_percentage'],
                trade_data['leverage'],
                trade_data['trade_type'],
                trade_data['entry_type'],
                trade_data['exit_reason']
            ))
            
            self.db.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing trade history: {e}")
            self.db.rollback()