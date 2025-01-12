import logging
import logging.handlers
import asyncio
from typing import Optional, List
from datetime import datetime
import os
import aiofiles
import async_timeout

class BufferedAsyncRotatingFileHandler(logging.Handler):
    def __init__(self, filename: str, max_bytes: int = 50*1024*1024,
                 backup_count: int = 10, encoding: str = 'utf-8',
                 buffer_size: int = 1000):
        super().__init__()
        self.baseFilename = filename  # filename을 baseFilename으로 저장
        self.filename = filename      # 원본 filename도 유지
        self.maxBytes = max_bytes
        self.backupCount = backup_count
        self.encoding = encoding
        self.queue = asyncio.Queue(maxsize=buffer_size)
        self.lock = asyncio.Lock()
        self._worker = None
        self._stopping = False
        self._buffer = []
        self._buffer_size = buffer_size
        self._last_flush = datetime.now()
        self._flush_interval = 5

    def acquire(self):
        """동기 메서드를 비동기로 오버라이드"""
        pass  # 실제 락은 async emit에서 처리
        
    def release(self):
        """동기 메서드를 비동기로 오버라이드"""
        pass  # 실제 락은 async emit에서 처리
        
    def do_rollover(self):
        """동기 롤오버 메서드"""
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = f"{self.filename}.{i}"
                dfn = f"{self.filename}.{i + 1}"
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = f"{self.filename}.1"
            if os.path.exists(dfn):
                os.remove(dfn)
            if os.path.exists(self.filename):
                os.rename(self.filename, dfn)
                
    def shouldRollover(self, record):
        """롤오버가 필요한지 확인"""
        if not os.path.exists(self.filename):
            return False
        if self.maxBytes > 0:
            try:
                size = os.path.getsize(self.filename)
                return size >= self.maxBytes
            except OSError:
                return False
        return False
        
    async def start(self) -> None:
        """비동기 워커 시작"""
        if self._worker is None:
            self._worker = asyncio.create_task(self._async_worker())
            await asyncio.sleep(0)  # 워커가 시작되도록 제어권 양보

    async def stop(self) -> None:
        """핸들러 정상 종료"""
        if not self._stopping:
            self._stopping = True
            try:
                # 남은 버퍼 처리
                if self._buffer:
                    for record in self._buffer:
                        await self.queue.put(record)
                    self._buffer.clear()
                
                # 큐가 비워질 때까지 대기
                if self._worker:
                    try:
                        async with async_timeout.timeout(5):  # 최대 5초 대기
                            while not self.queue.empty():
                                await asyncio.sleep(0.1)
                    except asyncio.TimeoutError:
                        logging.error("Timeout waiting for queue to empty")
                    
                    self._worker.cancel()
                    try:
                        await self._worker
                    except asyncio.CancelledError:
                        pass
                    self._worker = None
            except Exception as e:
                logging.error(f"Error during handler shutdown: {e}")

    def emit(self, record: logging.LogRecord) -> None:
        """로그 레코드를 버퍼에 추가"""
        try:
            # 중요 로그는 즉시 처리
            if record.levelno >= logging.ERROR:
                asyncio.create_task(self._async_emit(record))
                return

            if len(self._buffer) >= self._buffer_size:
                asyncio.create_task(self._flush_buffer())
            self._buffer.append(record)
            
            # 시간 기반 강제 플러시
            now = datetime.now()
            if (now - self._last_flush).seconds >= self._flush_interval:
                asyncio.create_task(self._flush_buffer())
        except Exception:
            self.handleError(record)

    async def _flush_buffer(self) -> None:
        """버퍼 비우기"""
        if not self._buffer:
            return

        try:
            while self._buffer:
                try:
                    record = self._buffer.pop(0)
                    await self.queue.put(record)
                except asyncio.QueueFull:
                    # 큐가 가득 찼다면 버퍼에 다시 추가
                    self._buffer.insert(0, record)
                    await asyncio.sleep(0.1)
                    break
            self._last_flush = datetime.now()
        except Exception as e:
            logging.error(f"Error during buffer flush: {e}")

    async def _async_worker(self) -> None:
        """비동기 로그 처리 워커"""
        while not self._stopping:
            try:
                # 배치 처리를 위한 레코드 수집
                records = []
                try:
                    # 첫 번째 레코드는 최대 1초까지 대기
                    record = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    records.append(record)
                    
                    # 추가 레코드가 있다면 최대 100개까지 즉시 수집
                    for _ in range(99):
                        try:
                            record = self.queue.get_nowait()
                            records.append(record)
                        except asyncio.QueueEmpty:
                            break
                            
                except asyncio.TimeoutError:
                    continue
                    
                if records:
                    async with self.lock:
                        for record in records:
                            try:
                                await self._async_emit(record)
                            finally:
                                self.queue.task_done()
                                
            except Exception as e:
                logging.error(f"Error in async worker: {e}")
                await asyncio.sleep(1)

    async def _async_emit(self, record: logging.LogRecord) -> None:
        """실제 로그 기록 처리"""
        try:
            msg = self.format(record)
            async with self.lock:
                # 파일 크기 확인 및 회전
                if self.shouldRollover(record):
                    await self._async_do_rollover()
                
                # 파일이 존재하지 않으면 생성
                if not os.path.exists(self.baseFilename):
                    async with aiofiles.open(self.baseFilename, 'a', encoding=self.encoding):
                        pass
                        
                async with aiofiles.open(self.baseFilename, 'a', encoding=self.encoding) as f:
                    await f.write(f"{msg}\n")
                    await f.flush()
                    os.fsync(f.fileno())  # 데이터 영구 저장 보장
                    
        except Exception as e:
            logging.error(f"Error in async emit: {e}")
            self.handleError(record)

    async def _async_do_rollover(self) -> None:
        """비동기 로그 파일 회전 처리"""
        if not os.path.exists(self.baseFilename):
            return

        async with self.lock:
            try:
                if self.stream:
                    self.stream.close()
                    self.stream = None

                # 임시 디렉토리에 백업
                temp_dir = os.path.join(os.path.dirname(self.baseFilename), '.temp')
                os.makedirs(temp_dir, exist_ok=True)

                # 기존 백업 파일 처리
                for i in range(self.backupCount - 1, 0, -1):
                    sfn = f"{self.baseFilename}.{i}"
                    dfn = f"{self.baseFilename}.{i + 1}"
                    try:
                        if os.path.exists(sfn):
                            if os.path.exists(dfn):
                                os.remove(dfn)
                            os.rename(sfn, dfn)
                    except Exception as e:
                        logging.error(f"Error during rollover: {e}")

                dfn = f"{self.baseFilename}.1"
                try:
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(self.baseFilename, dfn)
                except Exception as e:
                    logging.error(f"Error during rollover: {e}")

                # 새 로그 파일 생성
                async with aiofiles.open(self.baseFilename, 'a', encoding=self.encoding):
                    pass

            except Exception as e:
                logging.error(f"Error during rollover: {e}")

class AsyncAPILogHandler(BufferedAsyncRotatingFileHandler):
    """API 로그 전용 핸들러"""
    def __init__(self, filename: str, max_bytes: int = 50*1024*1024, 
                 backup_count: int = 10, encoding: str = 'utf-8'):
        super().__init__(filename, max_bytes, backup_count, encoding)
        self._last_logs = {}  # 중복 로그 제어를 위한 캐시
        self._cache_cleanup_task = None

    async def start(self) -> None:
        """캐시 정리 태스크 시작"""
        await super().start()
        if not self._cache_cleanup_task:
            self._cache_cleanup_task = asyncio.create_task(self._cleanup_cache())

    async def stop(self) -> None:
        """핸들러 정상 종료"""
        if self._cache_cleanup_task:
            self._cache_cleanup_task.cancel()
            try:
                await self._cache_cleanup_task
            except asyncio.CancelledError:
                pass
        await super().stop()

    async def _cleanup_cache(self) -> None:
        """오래된 캐시 항목 정리"""
        while True:
            try:
                await asyncio.sleep(60)  # 1분마다 실행
                current_time = datetime.now()
                # 1분 이상 된 캐시 항목 제거
                self._last_logs = {
                    k: v for k, v in self._last_logs.items()
                    if (current_time - v).total_seconds() < 60
                }
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error cleaning up API log cache: {e}")

    def emit(self, record: logging.LogRecord) -> None:
        """중복 필터링이 추가된 로그 발행"""
        try:
            # 로그 키 생성 (endpoint + method + status)
            log_key = (
                f"{getattr(record, 'url', '')}-"
                f"{getattr(record, 'method', '')}-"
                f"{getattr(record, 'status_code', '')}"
            )
            
            current_time = datetime.now()
            
            # 최소 로깅 간격 확인 (1초)
            if log_key in self._last_logs:
                time_diff = (current_time - self._last_logs[log_key]).total_seconds()
                if time_diff < 1:
                    return

            self._last_logs[log_key] = current_time
            super().emit(record)

        except Exception:
            self.handleError(record)

    def filter(self, record: logging.LogRecord) -> bool:
        """API 로그 필터링"""
        return all(hasattr(record, attr) for attr in ('url', 'method', 'status_code'))