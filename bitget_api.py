import aiohttp
import base64
import hmac
import hashlib
import time
import logging
import os
from typing import Optional, Dict
from urllib.parse import urlencode
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

class BitgetAPI:
    def __init__(self):
        self.API_KEY = os.getenv('BITGET_API_KEY')
        self.SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
        self.PASSPHRASE = os.getenv('BITGET_PASSPHRASE')
        self.BASE_URL = "https://api.bitget.com"
        self.session = None
        self.max_retries = 3
        self.retry_delay = 1  # Initial retry delay in seconds
        self.rate_limit_tokens = 10  # Rate limit token bucket
        self.rate_limit_last_update = time.time()
        self.rate_limit_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Context manager entry"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        await self.close()

    async def close(self):
        """Explicit session closure method"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _wait_for_rate_limit(self):
        """Rate limiting using token bucket algorithm"""
        async with self.rate_limit_lock:
            current_time = time.time()
            time_passed = current_time - self.rate_limit_last_update
            self.rate_limit_tokens = min(
                10,  # Max tokens
                self.rate_limit_tokens + time_passed * 2  # Token regeneration rate
            )
            
            if self.rate_limit_tokens < 1:
                wait_time = (1 - self.rate_limit_tokens) / 2
                await asyncio.sleep(wait_time)
                self.rate_limit_tokens = 1
            
            self.rate_limit_tokens -= 1
            self.rate_limit_last_update = current_time
    
    def _generate_signature(self, timestamp: str, method: str, 
                          request_path: str, body: str = '') -> str:
        """Generate API request signature"""
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            bytes(self.SECRET_KEY, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        return base64.b64encode(mac.digest()).decode()
    
    def _create_headers(self, method: str, request_path: str, body: str = '') -> dict:
        """Create API request headers"""
        timestamp = str(int(time.time() * 1000))
        
        # Sort query parameters
        if '?' in request_path:
            base_path, query = request_path.split('?', 1)
            params = sorted(query.split('&'))
            request_path = base_path + '?' + '&'.join(params)
        
        signature = self._generate_signature(timestamp, method.upper(), request_path, body)
        
        return {
            "ACCESS-KEY": self.API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.PASSPHRASE,
            "Content-Type": "application/json",
            "ACCESS-VERSION": "2"
        }
    
    async def _request(self, method: str, endpoint: str, 
                      params: dict = None, retry_count: int = 0) -> Optional[dict]:
        """Integrated API request handling with retry mechanism"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
        
        await self._wait_for_rate_limit()
        
        try:
            url = self.BASE_URL + endpoint
            query = ''
            
            if params:
                query = '?' + urlencode(sorted(params.items()))
                url = url + query
            
            headers = self._create_headers(method, endpoint + query)
            
            async with self.session.request(
                method=method,
                url=url,
                headers=headers
            ) as response:
                response_data = await response.json()
                
                if response.status == 429:  # Rate limit exceeded
                    if retry_count < self.max_retries:
                        wait_time = (2 ** retry_count) * self.retry_delay
                        await asyncio.sleep(wait_time)
                        return await self._request(method, endpoint, params, retry_count + 1)
                    
                elif response.status != 200:
                    if retry_count < self.max_retries and response.status >= 500:
                        wait_time = (2 ** retry_count) * self.retry_delay
                        await asyncio.sleep(wait_time)
                        return await self._request(method, endpoint, params, retry_count + 1)
                    logger.error(f"API Error: {response_data}")
                    
                return response_data
                
        except asyncio.TimeoutError:
            if retry_count < self.max_retries:
                wait_time = (2 ** retry_count) * self.retry_delay
                await asyncio.sleep(wait_time)
                return await self._request(method, endpoint, params, retry_count + 1)
            logger.error("Request timeout after all retries")
            return None
            
        except Exception as e:
            logger.error(f"Request error: {e}")
            if retry_count < self.max_retries:
                wait_time = (2 ** retry_count) * self.retry_delay
                await asyncio.sleep(wait_time)
                return await self._request(method, endpoint, params, retry_count + 1)
            return None
    
    async def get_position_ratio(self, symbol: str = 'BTCUSDT',
                               period: str = '1m') -> Optional[Dict]:
        """Get position long/short ratio with retry mechanism"""
        try:
            params = {
                'symbol': symbol,
                'period': period
            }
            
            response = await self._request(
                'GET', 
                '/api/v2/mix/market/account-long-short',
                params=params
            )
            
            if response and response.get('code') == '00000':
                data = response.get('data', [])
                if data:
                    latest = data[-1]
                    return {
                        'timestamp': int(time.time() * 1000),
                        'long_ratio': float(latest['longAccountRatio']),
                        'short_ratio': float(latest['shortAccountRatio']),
                        'long_short_ratio': float(latest['longShortAccountRatio']),
                        'success': True
                    }
            
            logger.error(f"Failed to get position ratio: {response}")
            return {
                'timestamp': int(time.time() * 1000),
                'success': False,
                'error': str(response)
            }
            
        except Exception as e:
            logger.error(f"Error fetching position ratio: {e}")
            return {
                'timestamp': int(time.time() * 1000),
                'success': False,
                'error': str(e)
            }