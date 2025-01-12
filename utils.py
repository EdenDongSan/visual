import time

class LogControlMixin:
    """로그 제어를 위한 Mixin 클래스"""
    
    def __init__(self):
        self._last_log_times = {}
        
    def should_log(self, key: str, interval: int = 60) -> bool:
        """주어진 키에 대해 로깅을 해야 하는지 확인
        
        Args:
            key: 로그 식별 키
            interval: 로깅 간격 (초)
            
        Returns:
            bool: 로깅 여부
        """
        current_time = time.time()
        
        # 첫 로그이거나 간격이 지났으면 로깅
        if (key not in self._last_log_times or 
            current_time - self._last_log_times[key] >= interval):
            self._last_log_times[key] = current_time
            return True
            
        return False