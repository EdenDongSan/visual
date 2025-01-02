import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional, Tuple
from database_manager import DatabaseManager, Candle
from data_api import BitgetAPI
import time
import math

logger = logging.getLogger(__name__)

class MarketDataManager:
    def __init__(self, api: BitgetAPI):
        self.api = api
        self.db_manager = DatabaseManager()
        self.latest_candle: Optional[Candle] = None
        self.candles_cache: Dict[int, Candle] = {}
        
        # OI와 L/S 데이터 캐시 수정
        self.position_ratio_cache: List[Dict[str, float]] = []  
        self.oi_cache: List[Tuple[int, float]] = []  
        self.max_oi_cache_size = 50  # 캐시 최대 크기 설정
        self.last_saved_ratio = None  # 마지막으로 저장된 L/S 비율
        self.last_saved_oi = None  # 마지막으로 저장된 OI 값
        
        self.ratio_update_interval = 60  # 60초
        self.oi_update_interval = 60     # 60초로 변경
        self.last_ratio_update = 0
        self.last_oi_update = 0          # 초기값 0으로 설정
        
        # 변경 임계값 설정
        self.oi_change_threshold = 0.00001  # 0.001% 변화
        self.ratio_change_threshold = 0.00001  # 0.001% 변화
        self._last_logged_values = {
            'oi': None,
            'ls_ratio': None,
            'volatility': None,
            'price': None,
            'leverage': None
        }   # 이전 로그값을 저장할 캐시

    async def initialize(self):
        """비동기 초기화"""
        await self._initialize_cache()
        await self.update_open_interest()  # OI 데이터 초기 로드

    async def _initialize_cache(self, lookback_minutes: int = 200) -> None:
        """초기 캐시 구성"""
        try:
            logger.info("Starting cache initialization from DB...")
            candles = self.db_manager.get_recent_candles(lookback_minutes)
            
            for candle in candles:
                self.candles_cache[candle.timestamp] = candle
            
            if candles:
                self.latest_candle = candles[0]  # 가장 최근 캔들
                
            logger.info(f"Successfully initialized cache with {len(candles)} candles")
                
        except Exception as e:
            logger.error(f"Error initializing cache from DB: {e}")

    async def update_latest_candle(self, candle: Candle) -> None:
        """새로운 캔들 데이터로 캐시 업데이트"""
        self.latest_candle = candle
        self.candles_cache[candle.timestamp] = candle
        
        # DB에 저장
        self.db_manager.store_candle(candle)

        # 캐시 크기 관리
        if len(self.candles_cache) > 200:
            oldest_timestamp = min(self.candles_cache.keys())
            del self.candles_cache[oldest_timestamp]

    def _has_significant_change(self, new_value: float, old_value: float, threshold: float) -> bool:
        """값의 유의미한 변화 여부 확인"""
        if old_value is None:
            return True
        return abs((new_value - old_value) / old_value) > threshold

    async def update_open_interest(self, symbol: str = 'BTCUSDT'):
        """OI 데이터 업데이트 - 유의미한 변화가 있을 때만"""
        current_time = int(time.time())
        
        if current_time - self.last_oi_update < self.oi_update_interval:
            return
            
        try:
            response = await self.api._request(
                'GET', 
                '/api/v2/mix/market/open-interest',
                params={
                    'symbol': symbol,
                    'productType': 'USDT-FUTURES'
                }
            )
            
            if response and response.get('code') == '00000':
                data = response['data']
                new_oi = float(data['openInterestList'][0]['size'])
                
                # 유의미한 변화가 있을 때만 저장
                if self._has_significant_change(new_oi, self.last_saved_oi, self.oi_change_threshold):
                    self.oi_cache.append((current_time * 1000, new_oi))
                    self.last_saved_oi = new_oi
                    self.last_oi_update = current_time
                    
                    # 캐시 크기 관리
                    if len(self.oi_cache) > self.max_oi_cache_size:
                        self.oi_cache.pop(0)
                        
                    # OI 값과 변화율 로깅 추가
                    oi_change = ((new_oi - self.last_saved_oi) / self.last_saved_oi * 100) if self.last_saved_oi else 0
                    logger.info(f"OI Update - Current: {new_oi:.2f}, Change: {oi_change:.2f}%, Cache Size: {len(self.oi_cache)}")

                     # OI 지표들 계산 및 로깅
                    indicators = self.calculate_oi_indicators()
                    logger.info(f"OI Indicators: {indicators}")
                    
        except Exception as e:
            logger.error(f"Error updating OI data: {e}")

    async def update_position_ratio(self, symbol: str = 'BTCUSDT'):
        """포지션 비율 데이터 업데이트 - 유의미한 변화가 있을 때만"""
        current_time = int(time.time())
        
        if current_time - self.last_ratio_update < self.ratio_update_interval:
            return
            
        ratios = await self.api.get_position_ratio(symbol)
        if ratios is not None:
            # 유의미한 변화 확인
            current_ls_ratio = ratios['long_short_ratio']
            
            if self._has_significant_change(current_ls_ratio, 
                                         self.last_saved_ratio, 
                                         self.ratio_change_threshold):
                # 타임스탬프 추가
                ratios['timestamp'] = current_time * 1000
                self.position_ratio_cache.append(ratios)
                self.last_saved_ratio = current_ls_ratio
                self.last_ratio_update = current_time
                
                # 최근 3개만 유지
                if len(self.position_ratio_cache) > 3:
                    self.position_ratio_cache.pop(0)
                    
                logger.info(f"New L/S ratio stored: {current_ls_ratio}")

    def calculate_trend_slope(self, data_points: List[Tuple[int, float]]) -> float:
        """추세선 기울기 계산"""
        if len(data_points) < 2:  # 최소 2개 필요
            return 0.0
            
        try:
            x = np.array([i for i in range(len(data_points))])
            y = np.array([point[1] for point in data_points])
            slope, _ = np.polyfit(x, y, 1)
            return slope
        except Exception as e:
            logger.error(f"Error calculating trend slope: {e}")
            return 0.0

    def calculate_ratio_acceleration(self) -> float:
        """L/S 비율 변화 가속도 계산 - 최근 3개 데이터 사용"""
        if len(self.position_ratio_cache) < 3:
            return 0.0
            
        try:
            ratios = [data['long_short_ratio'] for data in self.position_ratio_cache]
            velocities = np.diff(ratios)
            acceleration = np.diff(velocities)[0]  # 하나의 가속도 값만 계산
            return acceleration
        except Exception as e:
            logger.error(f"Error calculating ratio acceleration: {e}")
            return 0.0

    def get_latest_price(self) -> float:
        """현재 가격 조회"""
        return self.latest_candle.close if self.latest_candle else 0.0

    def get_recent_candles(self, lookback: int) -> List[Candle]:
        """최근 N개의 캔들 데이터 조회"""
        sorted_timestamps = sorted(self.candles_cache.keys(), reverse=True)
        return [self.candles_cache[ts] for ts in sorted_timestamps[:lookback]]

    def get_price_data_as_df(self, lookback: int) -> pd.DataFrame:
        """최근 N개의 캔들 데이터를 DataFrame으로 변환"""
        candles = self.get_recent_candles(lookback)
        data = {
            'timestamp': [c.timestamp for c in candles],
            'open': [c.open for c in candles],
            'high': [c.high for c in candles],
            'low': [c.low for c in candles],
            'close': [c.close for c in candles],
            'volume': [c.volume for c in candles]
        }
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.sort_values('timestamp')

    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        """EMA 계산"""
        return df['close'].ewm(span=period, adjust=False).mean()

    def calculate_stoch_rsi(self, period: int = 42, smoothk: int = 3, smoothd: int = 3) -> Tuple[float, float]:
        """Stochastic RSI 계산"""
        try:
            df = self.get_price_data_as_df(lookback=period*3)
            
            if len(df) < period*2:
                logger.warning(f"Insufficient data for Stoch RSI: {len(df)} < {period*2}")
                return 50.0, 50.0
            
            # RSI 계산
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # Stochastic RSI 계산
            rsi_min = rsi.rolling(window=period).min()
            rsi_max = rsi.rolling(window=period).max()
            stoch_rsi = 100 * (rsi - rsi_min) / (rsi_max - rsi_min)
            
            k = stoch_rsi.rolling(window=smoothk).mean()
            d = k.rolling(window=smoothd).mean()
            
            k_value = k.iloc[-1]
            d_value = d.iloc[-1]
            
            if np.isnan(k_value) or np.isnan(d_value):
                return 50.0, 50.0
                
            return k_value, d_value
                
        except Exception as e:
            logger.error(f"Error calculating Stoch RSI: {e}")
            return 50.0, 50.0

    def calculate_technical_indicators(self, lookback: int = 200) -> Dict[str, float]:
        """기술적 지표 계산"""
        try:
            result = {}
            
            # 1. 기본 가격 데이터 검증 및 계산
            df = self.get_price_data_as_df(lookback)
            if len(df) < 2:  # 최소 2개의 데이터 포인트 필요
                logger.warning(f"Insufficient price data: {len(df)} < 2")
                return {}

            # 2. 기본 가격 지표 계산
            try:
                result.update({
                    'last_close': float(df['close'].iloc[-1]),
                    'last_volume': float(df['volume'].iloc[-1]),
                    'price_change': float(df['close'].diff().iloc[-1]),
                    'ema7': float(self.calculate_ema(df, 7).iloc[-1]),
                    'ema25': float(self.calculate_ema(df, 25).iloc[-1]),
                    'ema200': float(self.calculate_ema(df, 200).iloc[-1])
                })
            except Exception as e:
                logger.error(f"Error calculating price indicators: {e}")

            # 3. Stoch RSI 계산
            try:
                stoch_k, stoch_d = self.calculate_stoch_rsi()
                result.update({
                    'stoch_k': float(stoch_k),
                    'stoch_d': float(stoch_d)
                })
            except Exception as e:
                logger.error(f"Error calculating Stoch RSI: {e}")
                result.update({'stoch_k': 50.0, 'stoch_d': 50.0})

            # 4. Market Indicators (포지션 비율 + OI 기본 지표) 계산
            try:
                market_inds = self.calculate_market_indicators()
                if market_inds:
                    result.update(market_inds)
            except Exception as e:
                logger.error(f"Error calculating market indicators: {e}")

            # 5. OI 상세 지표 계산 (충분한 데이터가 있을 때만)
            if len(self.oi_cache) >= 2:
                try:
                    oi_inds = self.calculate_oi_indicators()
                    if oi_inds:
                        # OI 지표들 중 숫자형 값만 추가
                        for key, value in oi_inds.items():
                            if isinstance(value, (int, float)):
                                result[f'oi_{key}'] = float(value)
                            elif key == 'is_anomaly':
                                result[f'oi_{key}'] = bool(value)

                    # OI 트렌드 시그널 계산 및 추가
                    oi_signals = self.get_oi_trend_signals()
                    if oi_signals:
                        for key, value in oi_signals.items():
                            if isinstance(value, (int, float)):
                                result[f'oi_signal_{key}'] = float(value)
                            else:
                                result[f'oi_signal_{key}'] = value

                except Exception as e:
                    logger.error(f"Error calculating OI details: {e}")
            else:
                logger.debug("Insufficient OI data for detailed indicators")

            # 6. 최종 결과 검증
            final_result = {}
            for key, value in result.items():
                try:
                    if isinstance(value, (int, float)):
                        if math.isnan(value) or math.isinf(value):
                            logger.warning(f"Invalid value detected for {key}, skipping")
                            continue
                    final_result[key] = value
                except Exception as e:
                    logger.error(f"Error validating {key}: {e}")
                    continue

            return final_result

        except Exception as e:
            logger.error(f"Error in calculate_technical_indicators: {str(e)}")
            return {}
        
    def calculate_atr(self, period: int = 14) -> float:
        """ATR 계산"""
        try:
            df = self.get_price_data_as_df(lookback=period*2)
            
            if len(df) < period:
                return 0.0
                
            df['high_low'] = df['high'] - df['low']
            df['high_pc'] = abs(df['high'] - df['close'].shift(1))
            df['low_pc'] = abs(df['low'] - df['close'].shift(1))
            
            df['tr'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)
            atr = df['tr'].rolling(window=period).mean().iloc[-1]
            
            # ATR 값이 20이상 차이나게 변경되었을 때만 로깅
            atr_value = float(atr)
            if not hasattr(self, '_last_logged_atr') or abs(self._last_logged_atr - atr_value) > 20:
                logger.info(f"ATR updated: {atr_value:.2f}")
                self._last_logged_atr = atr_value
            
            return atr_value
           
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return 0.0

    def calculate_position_ratio_indicators(self) -> Dict[str, float]:
        """포지션 비율 관련 지표 계산"""
        if not self.position_ratio_cache:
            return {
                'long_ratio': 0.5,
                'short_ratio': 0.5,
                'long_short_ratio': 1.0,
                'ratio_change_5m': 0.0,
                'ratio_change_15m': 0.0
            }
            
        current_data = self.position_ratio_cache[-1][1]
        current_time = self.position_ratio_cache[-1][0]
        
        def get_ratio_change(minutes: int) -> float:
            target_time = current_time - (minutes * 60 * 1000)
            for timestamp, data in reversed(self.position_ratio_cache[:-1]):
                if timestamp <= target_time:
                    return current_data['long_short_ratio'] - data['long_short_ratio']
            return 0.0
            
        return {
            'long_ratio': current_data['long_ratio'],
            'short_ratio': current_data['short_ratio'],
            'long_short_ratio': current_data['long_short_ratio'],
            'ratio_change_5m': get_ratio_change(5),
            'ratio_change_15m': get_ratio_change(15)
        }
    
    def calculate_market_indicators(self) -> Dict[str, float]:
        """시장 지표 계산"""
        try:
            # OI 추세선 기울기
            if len(self.oi_cache) >= 2:
                oi_data = [(ts, value) for ts, value in self.oi_cache]
                oi_slope = self.calculate_trend_slope(oi_data)
            else:
                oi_slope = 0.0
            
            # L/S 비율 관련 계산
            if len(self.position_ratio_cache) >= 2:
                ls_ratio_data = [(data['timestamp'], data['long_short_ratio']) 
                               for data in self.position_ratio_cache]
                ls_ratio_slope = self.calculate_trend_slope(ls_ratio_data)
            else:
                ls_ratio_slope = 0.0
                
            ls_ratio_acceleration = self.calculate_ratio_acceleration()
            
            # 현재 L/S 비율 데이터
            current_ratios = self.position_ratio_cache[-1] if self.position_ratio_cache else {
                'long_ratio': 50.0,
                'short_ratio': 50.0,
                'long_short_ratio': 1.0
            }
            
            return {
                'oi_slope': oi_slope,
                'ls_ratio_slope': ls_ratio_slope,
                'ls_ratio_acceleration': ls_ratio_acceleration,
                'current_long_ratio': current_ratios.get('long_ratio', 50.0),
                'current_short_ratio': current_ratios.get('short_ratio', 50.0),
                'current_ls_ratio': current_ratios.get('long_short_ratio', 1.0)
            }
            
        except Exception as e:
            logger.error(f"Error calculating market indicators: {e}")
            return {
                'oi_slope': 0.0,
                'ls_ratio_slope': 0.0,
                'ls_ratio_acceleration': 0.0,
                'current_long_ratio': 50.0,
                'current_short_ratio': 50.0,
                'current_ls_ratio': 1.0
            }
        
    def calculate_oi_indicators(self) -> dict:
        """OI 관련 지표들 계산"""
        try:
            # 최소 필요 데이터 수 확인
            if len(self.oi_cache) < 2:
                logger.warning("Not enough OI data points")
                return {}

            # numpy array로 변환하여 계산 효율성 향상
            timestamps = np.array([ts for ts, _ in self.oi_cache])
            oi_values = np.array([value for _, value in self.oi_cache])
            
            # 기본 통계
            current_oi = float(oi_values[-1])
            oi_mean = float(np.mean(oi_values))
            oi_std = float(np.std(oi_values)) if len(oi_values) > 1 else 0.0
            
            # 변화율 계산 (퍼센트)
            oi_changes = np.diff(oi_values) / oi_values[:-1] * 100
            recent_change_pct = float(oi_changes[-1]) if len(oi_changes) > 0 else 0.0
            
            # 이동평균 계산
            ma_5 = float(np.mean(oi_values[-5:])) if len(oi_values) >= 5 else current_oi
            ma_20 = float(np.mean(oi_values[-20:])) if len(oi_values) >= 20 else current_oi
            
            # 추세 강도 계산
            if len(oi_values) >= 2:
                x = np.arange(len(oi_values))
                slope, _ = np.polyfit(x, oi_values, 1)
                slope = float(slope)
                slope_strength = abs(slope) / oi_std if oi_std != 0 else 0
            else:
                slope = 0.0
                slope_strength = 0.0
            
            # 변동성 계산
            volatility = float(oi_std / oi_mean) if oi_mean != 0 else 0
            
            # 모멘텀 지표 계산
            momentum_5 = float((current_oi / ma_5 - 1) * 100) if ma_5 != 0 else 0
            momentum_20 = float((current_oi / ma_20 - 1) * 100) if ma_20 != 0 else 0
            
            # 이상치 탐지
            z_score = float((current_oi - oi_mean) / oi_std) if oi_std != 0 else 0
            is_anomaly = abs(z_score) > 2
            
            # RSI 계산 (OI 기반)
            if len(oi_changes) >= 14:
                gains = np.where(oi_changes > 0, oi_changes, 0)
                losses = np.where(oi_changes < 0, -oi_changes, 0)
                avg_gain = np.mean(gains[-14:])
                avg_loss = np.mean(losses[-14:])
                if avg_loss != 0:
                    rs = avg_gain / avg_loss
                    oi_rsi = 100 - (100 / (1 + rs))
                else:
                    oi_rsi = 100 if avg_gain != 0 else 50
            else:
                oi_rsi = 50

            # 결과 딕셔너리 생성
            result = {
                'current_oi': current_oi,
                'oi_mean': oi_mean,
                'oi_std': oi_std,
                'recent_change_pct': recent_change_pct,
                'ma5': ma_5,
                'ma20': ma_20,
                'slope': slope,
                'slope_strength': float(slope_strength),
                'volatility': volatility,
                'momentum_5': momentum_5,
                'momentum_20': momentum_20,
                'z_score': z_score,
                'is_anomaly': is_anomaly,
                'oi_rsi': float(oi_rsi)
            }

            # 결과값 검증
            for key, value in result.items():
                if isinstance(value, (int, float)) and (math.isnan(value) or math.isinf(value)):
                    result[key] = 0.0
                    logger.warning(f"Invalid value detected for {key}, reset to 0")

            return result
            
        except Exception as e:
            logger.error(f"Error calculating OI indicators: {e}")
            return {}
        
    def get_oi_trend_signals(self) -> dict:
        """OI 기반 트렌드 시그널 생성"""
        try:
            indicators = self.calculate_oi_indicators()
            if not indicators:
                return {
                    'trend': 'neutral',
                    'strength': 0.0,
                    'momentum': 'neutral',
                    'volatility': 'normal',
                    'warning': False
                }

            # 기본값 설정
            signals = {
                'trend': 'neutral',
                'strength': 0.0,
                'momentum': 'neutral',
                'volatility': 'normal',
                'warning': False
            }

            # 트렌드 방향 및 강도 판단
            slope = indicators['slope']
            momentum_5 = indicators['momentum_5']
            momentum_20 = indicators['momentum_20']
            oi_rsi = indicators['oi_rsi']
            
            # RSI 기반 과매수/과매도 체크
            is_overbought = oi_rsi > 70
            is_oversold = oi_rsi < 30

            # 트렌드 판단
            if slope > 0:
                if momentum_5 > 0.5 and momentum_20 > 0:
                    signals['trend'] = 'strong_bullish'
                else:
                    signals['trend'] = 'bullish'
            elif slope < 0:
                if momentum_5 < -0.5 and momentum_20 < 0:
                    signals['trend'] = 'strong_bearish'
                else:
                    signals['trend'] = 'bearish'

            # 트렌드 강도 계산
            signals['strength'] = float(indicators['slope_strength'])

            # 모멘텀 상태 판단
            if momentum_5 >= 0.5:
                signals['momentum'] = 'strong_bullish'
            elif momentum_5 <= -0.5:
                signals['momentum'] = 'strong_bearish'
            elif momentum_5 >= 0.2:
                signals['momentum'] = 'bullish'
            elif momentum_5 <= -0.2:
                signals['momentum'] = 'bearish'

            # 변동성 상태 판단
            volatility = indicators['volatility']
            if volatility > 0.15:
                signals['volatility'] = 'very_high'
            elif volatility > 0.1:
                signals['volatility'] = 'high'
            elif volatility < 0.03:
                signals['volatility'] = 'very_low'
            elif volatility < 0.05:
                signals['volatility'] = 'low'

            # 경고 신호 생성
            signals['warning'] = (
                indicators['is_anomaly'] or
                volatility > 0.15 or
                abs(indicators['z_score']) > 2.5 or
                is_overbought or
                is_oversold
            )

            # 추가 상태 정보
            signals['is_overbought'] = is_overbought
            signals['is_oversold'] = is_oversold
            signals['rsi'] = float(oi_rsi)

            return signals

        except Exception as e:
            logger.error(f"Error generating OI trend signals: {e}")
            return {
                'trend': 'neutral',
                'strength': 0.0,
                'momentum': 'neutral',
                'volatility': 'normal',
                'warning': False
            }