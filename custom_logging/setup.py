import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import os
from dotenv import load_dotenv
import asyncio

from .handlers import BufferedAsyncRotatingFileHandler, AsyncAPILogHandler

logger = logging.getLogger(__name__)

async def setup_logging(log_dir: str = "logs", env_path: Optional[str] = None) -> Dict[str, BufferedAsyncRotatingFileHandler]:
    """비동기 로깅 설정"""
    # .env 파일 로드
    load_dotenv(env_path)
    
    # 로그 레벨 설정
    log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))
    
    # 로그 디렉토리 생성
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # 현재 날짜로 로그 파일명 생성
    current_date = datetime.now().strftime("%Y%m%d")
    
    # 각 로그 파일 설정
    log_files = {
        'main': f"{current_date}_trading_bot.log",
        'error': f"{current_date}_error.log",
        'trades': f"{current_date}_trades.log",
        'api': f"{current_date}_api.log",
        'websocket': f"{current_date}_websocket.log"
    }
    
    # 로그 파일 경로 생성
    log_paths = {key: os.path.join(log_dir, filename) 
                for key, filename in log_files.items()}
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 포맷터 설정
    formatters = {
        'detailed': logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ),
        'api': logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(status_code)s] %(url)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    }
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatters['detailed'])
    root_logger.addHandler(console_handler)
    
    root_logger.info("Async logging system initialized")

    # 비동기 핸들러들 설정
    handlers = {}
    for log_type, path in log_paths.items():
        try:
            if log_type == 'api':
                handler = AsyncAPILogHandler(path)
            else:
                handler = BufferedAsyncRotatingFileHandler(path)
                
            handler.setLevel(logging.ERROR if log_type == 'error' else log_level)
            handler.setFormatter(
                formatters['api'] if log_type == 'api' else formatters['detailed']
            )
            await handler.start()
            handlers[log_type] = handler
            root_logger.addHandler(handler)
            
        except Exception as e:
            print(f"Error setting up {log_type} handler: {e}")
            
    return handlers

async def cleanup_logging(handlers: Dict[str, BufferedAsyncRotatingFileHandler]) -> None:
    """로깅 시스템 정리"""
    try:
        # 각 핸들러에 대해 최대 5초간 대기하며 정리
        for handler in handlers.values():
            try:
                await asyncio.wait_for(handler.stop(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Handler cleanup timed out for {handler.filename}")
            except asyncio.CancelledError:
                # 취소 에러는 정상적으로 처리
                pass
            except Exception as e:
                logger.error(f"Error during handler cleanup: {e}")
    except Exception as e:
        logger.error(f"Error during logging cleanup: {e}")