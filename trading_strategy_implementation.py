import logging
from typing import Optional, Tuple
from dataclasses import dataclass
import asyncio
import time
from order_execution import OrderExecutor
from market_data_manager import MarketDataManager
from models import Position, TradingMetrics
import math
from models import MarketData

logger = logging.getLogger(__name__)

@dataclass
class TradingConfig:
    leverage: int = 10  # 기본 레버리지
    max_leverage: int = 20  # 최대 레버리지
    position_size_pct: float = 95.0  # 전체 자산의 92%
    stop_loss_pct: float = 10.0  # 최대 손실 제한
    ratio_threshold: float = 0.02  # L/S 비율 변화 임계값
    min_slope: float = 0.0001  # 최소 기울기 요구사항
    acceleration_threshold: float = 0.0001  # 가속도 임계값

class TradingStrategy:
    def __init__(self, market_data: MarketDataManager, order_executor: OrderExecutor):
        self.market_data = market_data
        self.order_executor = order_executor
        self.config = TradingConfig()
        self.metrics = TradingMetrics()
        self.in_position = False
        self.last_trade_time = 0
        self.min_trade_interval = 120

    async def calculate_position_size(self, current_price: float) -> float:
        """계좌 잔고를 기반으로 포지션 크기 계산"""
        try:
            account_info = await self.order_executor.api.get_account_balance()
            
            if account_info.get('code') != '00000':
                logger.error(f"Failed to get account balance: {account_info}")
                return 0.0
            
            account_data = account_info.get('data', [])[0]
            available_balance = float(account_data.get('available', '0'))
            
            # 전체 잔고의 92% 사용
            trade_amount = available_balance * (self.config.position_size_pct / 100)
            position_size = (trade_amount * self.config.leverage) / current_price
            floor_size = math.floor(position_size * 1000) / 1000  # 소수점 3자리까지
            
            logger.info(f"Calculated position size: {floor_size} (Available balance: {available_balance})")
            return floor_size
                
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0.0

    def should_open_long(self, indicators: dict, market_indicators: dict) -> bool:
        """롱 포지션 진입 조건 확인"""
        try:
            # 주요 조건 확인
            ls_ratio_slope = market_indicators.get('ls_ratio_slope', 0)
            ls_ratio_acceleration = market_indicators.get('ls_ratio_acceleration', 0)
            
            # 가격 트렌드 확인
            current_price = float(indicators['last_close'])
            ema200 = float(indicators['ema200'])
            price_above_ema = current_price > ema200
            price_change = float(indicators['price_change'])
            price_rising = price_change > 0
            
            # L/S 비율 조건
            ratio_trending_up = (ls_ratio_slope > self.config.min_slope and 
                            ls_ratio_acceleration > self.config.acceleration_threshold)
            
            # OI RSI 과매수 확인 (롱 진입 조건으로 변경)
            oi_rsi = indicators.get('oi_oi_rsi', indicators.get('oi_rsi', 50.0))
            is_overbought = oi_rsi > 70

            # 1분마다 한 번씩만 로깅
            current_time = int(time.time())
            if not hasattr(self, '_last_condition_log_time_long') or current_time - self._last_condition_log_time_long >= 60:
                self._last_condition_log_time_long = current_time
                logger.info("\n=== LONG ENTRY CONDITIONS LOG ===")
                logger.info(f"Current Price: {current_price:.2f} vs EMA200: {ema200:.2f} (Above EMA: {price_above_ema})")
                logger.info(f"Price Change: {price_change:.2f} (Rising: {price_rising})")
                logger.info(f"L/S Ratio Slope: {ls_ratio_slope:.6f} (Min Required: {self.config.min_slope})")
                logger.info(f"L/S Ratio Acceleration: {ls_ratio_acceleration:.6f} (Min Required: {self.config.acceleration_threshold})")
                logger.info(f"OI RSI: {oi_rsi:.2f} (Overbought: {is_overbought})")
                logger.info(f"Ratio Trending Up: {ratio_trending_up}")
                logger.info(f"Already In Position: {self.in_position}")
                logger.info("================================")

            should_enter = (
                ratio_trending_up and
                price_above_ema and
                price_rising and
                is_overbought and  # 변경된 부분
                not self.in_position
            )
            
            return should_enter
            
        except Exception as e:
            # 에러 로깅 횟수 제한
            current_time = int(time.time())
            if not hasattr(self, '_last_long_error_time') or current_time - self._last_long_error_time >= 60:
                self._last_long_error_time = current_time
                logger.error(f"Error in should_open_long: {e}")
            return False

    def should_open_short(self, indicators: dict, market_indicators: dict) -> bool:
        """숏 포지션 진입 조건 확인"""
        try:
            # 주요 조건 확인
            ls_ratio_slope = market_indicators.get('ls_ratio_slope', 0)
            ls_ratio_acceleration = market_indicators.get('ls_ratio_acceleration', 0)
            
            # 가격 트렌드 확인
            current_price = float(indicators['last_close'])
            ema200 = float(indicators['ema200'])
            price_below_ema = current_price < ema200
            price_change = float(indicators['price_change'])
            price_falling = price_change < 0
            
            # L/S 비율 조건
            ratio_trending_down = (ls_ratio_slope < -self.config.min_slope and 
                                ls_ratio_acceleration < -self.config.acceleration_threshold)
            
            # OI RSI 과매도 확인 (숏 진입 조건으로 변경)
            oi_rsi = indicators.get('oi_oi_rsi', indicators.get('oi_rsi', 50.0))
            is_oversold = oi_rsi < 30

            # 1분마다 한 번씩만 로깅
            current_time = int(time.time())
            if not hasattr(self, '_last_condition_log_time_short') or current_time - self._last_condition_log_time_short >= 60:
                self._last_condition_log_time_short = current_time
                logger.info("\n=== SHORT ENTRY CONDITIONS LOG ===")
                logger.info(f"Current Price: {current_price:.2f} vs EMA200: {ema200:.2f} (Below EMA: {price_below_ema})")
                logger.info(f"Price Change: {price_change:.2f} (Falling: {price_falling})")
                logger.info(f"L/S Ratio Slope: {ls_ratio_slope:.6f} (Min Required: {-self.config.min_slope})")
                logger.info(f"L/S Ratio Acceleration: {ls_ratio_acceleration:.6f} (Min Required: {-self.config.acceleration_threshold})")
                logger.info(f"OI RSI: {oi_rsi:.2f} (Oversold: {is_oversold})")
                logger.info(f"Ratio Trending Down: {ratio_trending_down}")
                logger.info(f"Already In Position: {self.in_position}")
                logger.info("================================")

            should_enter = (
                ratio_trending_down and
                price_below_ema and
                price_falling and
                is_oversold and  # 변경된 부분
                not self.in_position
            )
            
            return should_enter
            
        except Exception as e:
            # 에러 로깅 횟수 제한
            current_time = int(time.time())
            if not hasattr(self, '_last_short_error_time') or current_time - self._last_short_error_time >= 60:
                self._last_short_error_time = current_time
                logger.error(f"Error in should_open_short: {e}")
            return False
        
    async def should_close_position(self, position: Position, 
                              indicators: dict, 
                              market_indicators: dict) -> Tuple[bool, str]:
        """포지션 청산 조건 확인"""
        try:
            if not isinstance(position, Position):
                position = await position
                if not position:
                    return False, ""
            
            # 1. 손실 제한 확인
            unrealized_pnl = float(position.unrealized_pl)
            if unrealized_pnl <= -self.config.stop_loss_pct:
                logger.info(f"Stop loss triggered: {unrealized_pnl}%")
                return True, "stop_loss"
            
            oi_rsi = indicators.get('oi_oi_rsi', indicators.get('oi_rsi', 50.0))
            if  (position.side == 'long' and oi_rsi <= 30) or \
                (position.side == 'short' and oi_rsi >= 80):
                logger.info(f"OI RSI condition met for closing: {oi_rsi}")
                return True, "oi_rsi_condition"
            
            # 3. L/S 비율 반전 확인
            ls_ratio_slope = market_indicators.get('ls_ratio_slope', 0)
            ls_ratio_acceleration = market_indicators.get('ls_ratio_acceleration', 0)
            
            if position.side == 'long':
                ratio_reversal = (ls_ratio_slope < -self.config.min_slope and 
                                ls_ratio_acceleration < -self.config.acceleration_threshold)
            else:
                ratio_reversal = (ls_ratio_slope > self.config.min_slope and 
                                ls_ratio_acceleration > self.config.acceleration_threshold)
            
            if ratio_reversal:
                logger.info("Trend reversal detected")
                return True, "trend_reversal"
            
            return False, ""
        
        except Exception as e:
            logger.error(f"Error in should_close_position: {e}")
            return False, ""

    async def execute_entry(self, side: str, current_price: float):
        """전체 포지션 진입 실행 (스탑로스만 설정)"""
        try:
            total_size = await self.calculate_position_size(current_price)
            
            entry_price = current_price
            # 롱/숏에 따른 스탑로스 가격만 설정
            if side == "long":
                stop_loss_price = entry_price * (1 - self.config.stop_loss_pct/100)
            else:  # short
                stop_loss_price = entry_price * (1 + self.config.stop_loss_pct/100)
            
            success = await self.order_executor.open_position(
                symbol="BTCUSDT",
                side=side,
                size=str(total_size),
                leverage=self.config.leverage,
                stop_loss_price=stop_loss_price,
                take_profit_price=0.0,  # 테이크프로핏은 0으로 설정하여 비활성화
                current_price=current_price,
                order_type='limit',
                price=str(entry_price)
            )
            
            if success:
                # 거래 기록 저장
                trade_data = {
                    'timestamp': int(time.time() * 1000),
                    'symbol': "BTCUSDT",
                    'side': side,
                    'size': float(total_size),
                    'entry_price': float(entry_price),
                    'exit_price': 0.0,  # 진입 시에는 0
                    'pnl': 0.0,  # 진입 시에는 0
                    'pnl_percentage': 0.0,  # 진입 시에는 0
                    'leverage': self.config.leverage,
                    'trade_type': 'limit',
                    'entry_type': 'trend_follow',  # 전략에 따라 수정 가능
                    'exit_reason': ''  # 진입 시에는 빈 문자열
                }
                
                self.market_data.db_manager.store_trade(trade_data)
                logger.info(f"Trade entry recorded: {side} {total_size} @ {entry_price}")
                
                self.in_position = True
                self.last_trade_time = int(time.time())
                logger.info(f"Successfully placed {side.upper()} position with stop loss at {stop_loss_price}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error executing entry: {e}")
            return False

    async def execute_close(self, position: Position, reason: str) -> bool:
        """전체 포지션 청산 실행"""
        try:
            logger.info(f"Closing position: {position.symbol}, reason: {reason}")
            
            # 현재 가격 정보 가져오기
            current_price = self.market_data.get_latest_price()
            
            close_success = await self.order_executor.execute_market_close(position)
            if close_success:
                # PnL 계산
                if position.side == 'long':
                    pnl = (current_price - position.entry_price) * position.size
                    pnl_percentage = ((current_price - position.entry_price) / position.entry_price) * 100
                else:  # short
                    pnl = (position.entry_price - current_price) * position.size
                    pnl_percentage = ((position.entry_price - current_price) / position.entry_price) * 100

                # 거래 기록 저장
                trade_data = {
                    'timestamp': int(time.time() * 1000),
                    'symbol': position.symbol,
                    'side': position.side,
                    'size': position.size,
                    'entry_price': position.entry_price,
                    'exit_price': current_price,
                    'pnl': pnl,
                    'pnl_percentage': pnl_percentage,
                    'leverage': position.leverage,
                    'trade_type': 'market',
                    'entry_type': 'trend_follow',
                    'exit_reason': reason
                }
                
                self.market_data.db_manager.store_trade(trade_data)
                logger.info(f"Position fully closed with market order. Reason: {reason}")
                logger.info(f"Trade exit recorded - PnL: {pnl:.2f} USDT ({pnl_percentage:.2f}%)")
                
                # 트레이딩 지표 업데이트
                self.metrics.update(pnl)
                
                self.in_position = False
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error executing close: {e}")
            return False

    def _adjust_leverage(self, volatility: float) -> int:
        """변동성에 따른 레버리지 동적 조정"""
        try:
            base_volatility = 100
            volatility_ratio = base_volatility / volatility if volatility > 0 else 1
            
            adjusted_leverage = int(self.config.max_leverage * volatility_ratio)
            adjusted_leverage = max(5, min(adjusted_leverage, self.config.max_leverage))
            
                # 이전 값과 다를 때만 로깅
            if not hasattr(self, '_last_logged_leverage') or self._last_logged_leverage != adjusted_leverage:
                logger.info(f"Adjusted leverage: {adjusted_leverage} (volatility: {volatility:.2f})")
                self._last_logged_leverage = adjusted_leverage
            return adjusted_leverage
            
        except Exception as e:
            logger.error(f"Error adjusting leverage: {e}")
            return self.config.leverage

    async def run(self):
        """전략 실행 메인 루프"""
        logger.info("Starting trading strategy")
        while True:
            try:
                await self.market_data.update_position_ratio("BTCUSDT")
                await self.market_data.update_open_interest("BTCUSDT")
                await self.market_data.store_market_indicators()
                await self._process_trading_logic()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)

    async def _process_trading_logic(self):
        """트레이딩 로직 처리"""
        try:
            # 데이터 업데이트
            await self.market_data.update_position_ratio("BTCUSDT")
            await self.market_data.update_open_interest("BTCUSDT")
            
            # 현재 포지션 확인
            position = await self.order_executor.get_position("BTCUSDT")
            
            # 기술적 지표 계산
            indicators = self.market_data.calculate_technical_indicators()
            if not indicators:
                return
                
            # 시장 지표 계산
            market_indicators = self.market_data.calculate_market_indicators()
            if not market_indicators:
                return

            current_time = int(time.time())
            current_price = indicators.get('last_close')
            
            if not current_price:
                return

            # 포지션이 있는 경우 - 청산 조건만 확인
            if position and position.size > 0:
                should_close, close_reason = await self.should_close_position(
                    position, indicators, market_indicators
                )
                
                if should_close:
                    await self.execute_close(position, close_reason)
                    
           # 포지션이 없는 경우에만 진입 조건 확인
            elif not self.in_position and (current_time - self.last_trade_time) >= self.min_trade_interval:
                # 레버리지 동적 조정
                volatility = self.market_data.calculate_atr(period=14)
                adjusted_leverage = self._adjust_leverage(volatility)
                self.config.leverage = adjusted_leverage
                
                # 롱과 숏 진입 조건 모두 확인
                long_condition = self.should_open_long(indicators, market_indicators)
                short_condition = self.should_open_short(indicators, market_indicators)
                
                # 진입 실행
                if long_condition:
                    await self.execute_entry("long", current_price)
                elif short_condition:
                    await self.execute_entry("short", current_price)
            
        except Exception as e:
            logger.error(f"Error in trading logic: {e}")