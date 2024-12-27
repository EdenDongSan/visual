import asyncio
import websockets
import json
import logging
from typing import Optional, Dict, List
from database_manager import DatabaseManager, Candle
from bitget_api import BitgetAPI

logger = logging.getLogger(__name__)

class MarketDataManager:
    def __init__(self):
        self.WS_URL = "wss://ws.bitget.com/v2/ws/public"
        self.ws = None
        self.connected = False
        self.db_manager = DatabaseManager()
        self.api = None
        self.callbacks = []
        self.should_run = True
        self.reconnect_delay = 1  # Initial reconnect delay
        self.max_reconnect_delay = 60  # Maximum reconnect delay
        self._ws_lock = asyncio.Lock()  # Lock for WebSocket connection sync
        self.last_ping_time = 0
        self.ping_interval = 30  # Send ping every 30 seconds
    
    def add_callback(self, callback):
        """Register callback"""
        if callback not in self.callbacks:
            self.callbacks.append(callback)
    
    def remove_callback(self, callback):
        """Remove callback"""
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    async def notify_callbacks(self, data):
        """Execute all registered callbacks"""
        for callback in self.callbacks:
            try:
                await callback(data)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
    
    async def connect_websocket(self) -> bool:
        """Connect WebSocket and subscribe"""
        async with self._ws_lock:
            try:
                if self.ws:
                    await self.ws.close()
                
                self.ws = await websockets.connect(
                    self.WS_URL,
                    ping_interval=None,  # Disable default ping
                    close_timeout=5
                )
                
                # Subscribe message
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [{
                        "channel": "candle1m",
                        "instId": "BTCUSDT",
                        "instType": "USDT-FUTURES"
                    }]
                }
                await self.ws.send(json.dumps(subscribe_msg))
                
                # Wait for subscription confirmation
                response = await self.ws.recv()
                response_data = json.loads(response)
                
                if response_data.get('event') == 'subscribe':
                    self.connected = True
                    self.reconnect_delay = 1  # Reset reconnect delay on success
                    logger.info("WebSocket connected and subscribed")
                    return True
                
                return False
                
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                self.connected = False
                return False

    async def send_ping(self):
        """Send ping message"""
        try:
            if self.ws and self.connected:
                await self.ws.send('ping')
                self.last_ping_time = asyncio.get_event_loop().time()
        except Exception as e:
            logger.error(f"Error sending ping: {e}")
            self.connected = False
    
    async def handle_websocket_message(self, message: str):
        """Handle WebSocket messages"""
        try:
            if message == 'pong':
                return
            
            if isinstance(message, str):
                data = json.loads(message)
                
                # Handle different message types
                if 'data' in data and isinstance(data['data'], list):
                    candle_data = data['data'][0]
                    if isinstance(candle_data, list) and len(candle_data) >= 6:
                        candle = await self.process_candle_data(candle_data)
                        if candle:
                            await self.notify_callbacks({
                                'type': 'candle',
                                'data': candle
                            })
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Message handling error: {e}")
    
    async def run(self):
        """Main execution function"""
        try:
            async with BitgetAPI() as self.api:
                while self.should_run:
                    if not self.connected:
                        if not await self.connect_websocket():
                            await asyncio.sleep(self.reconnect_delay)
                            self.reconnect_delay = min(
                                self.reconnect_delay * 2,
                                self.max_reconnect_delay
                            )
                            continue
                    
                    try:
                        # Check if it's time to send a ping
                        current_time = asyncio.get_event_loop().time()
                        if current_time - self.last_ping_time >= self.ping_interval:
                            await self.send_ping()
                        
                        # Set timeout for receiving messages
                        async with asyncio.timeout(self.ping_interval):
                            message = await self.ws.recv()
                            await self.handle_websocket_message(message)
                            
                    except asyncio.TimeoutError:
                        logger.warning("WebSocket receive timeout")
                        self.connected = False
                    except websockets.ConnectionClosed:
                        logger.warning("WebSocket connection closed")
                        self.connected = False
                    except Exception as e:
                        logger.error(f"Error in main loop: {e}")
                        self.connected = False
                        await asyncio.sleep(1)
                        
        except Exception as e:
            logger.error(f"Fatal error in run: {e}")
        finally:
            self.should_run = False
            if self.ws:
                await self.ws.close()
    
        async def process_candle_data(self, data: list) -> Optional[Candle]:
            """Process candle data"""
            try:
                timestamp = int(float(data[0]))
                candle = Candle(
                    timestamp=timestamp,
                    open=float(data[1]),
                    high=float(data[2]),
                    low=float(data[3]),
                    close=float(data[4]),
                    volume=float(data[5]),
                    quote_volume=float(data[6]) if len(data) > 6 else None
                )
                
                # Store in DB
                self.db_manager.store_candle(candle)
                return candle
                
            except Exception as e:
                logger.error(f"Error processing candle data: {e}")
                return None
        
        def stop(self):
            """Stop execution"""
            self.should_run = False
        
        async def fetch_and_store_ratio(self):
            """Fetch and store long/short ratio data"""
            try:
                ratio_data = await self.api.get_position_ratio()
                if ratio_data:
                    self.db_manager.store_long_short_ratio(
                        ratio_data['timestamp'],
                        ratio_data['long_ratio'],
                        ratio_data['short_ratio'],
                        ratio_data['long_short_ratio']
                    )
                    await self.notify_callbacks({
                        'type': 'ratio',
                        'data': ratio_data
                    })
            except Exception as e:
                logger.error(f"Error fetching ratio data: {e}")
        
        def get_recent_data(self, limit: int = 200):
            """Get recent data"""
            return {
                'candles': self.db_manager.get_recent_candles(limit),
                'ratios': self.db_manager.get_recent_ratio(limit)
            }