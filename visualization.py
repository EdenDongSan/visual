import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk
from datetime import datetime
from typing import List, Dict, Optional
import numpy as np
from database_manager import Candle
import logging
import time
import queue
import threading
import os
import matplotlib.ticker as ticker

logger = logging.getLogger(__name__)

class MarketDataVisualization:
    def __init__(self, max_points: int = 1000):
        """
        시장 데이터 시각화 클래스 초기화
        
        Args:
            max_points (int): 차트에 표시할 최대 데이터 포인트 수
        """
        self.max_points = max_points
        self.update_queue = queue.Queue()
        self.data_lock = threading.Lock()
        
        # 자동 저장 설정
        self.auto_save = True
        self.save_interval_hours = 2  # 2시간 간격
        self.last_save_time = time.time()
        self.save_folder = "chart_images"
        self.is_shutting_down = False
        
        # 데이터 저장소
        self.candle_data = []
        self.ratio_data = []
        
        # GUI 업데이트 제어
        self.last_update = time.time()
        self.update_interval = 1.0
        
        # 저장 폴더 생성
        os.makedirs(self.save_folder, exist_ok=True)
        logger.info(f"Created chart save directory: {self.save_folder}")
        
        self._setup_gui_thread()
        self._start_auto_save()
    
    def _setup_gui_thread(self):
        """GUI 스레드 설정 및 시작"""
        self.gui_thread = threading.Thread(target=self._run_gui, daemon=True)
        self.gui_ready = threading.Event()
        self.gui_thread.start()
        self.gui_ready.wait()
    
    def _start_auto_save(self):
        """자동 저장 스레드 시작"""
        save_thread = threading.Thread(target=self._auto_save_loop, daemon=True)
        save_thread.start()
        logger.info(f"Started auto-save with {self.save_interval_hours} hour interval")
    
    def _run_gui(self):
        """GUI 스레드 메인 루프"""
        try:
            self.root = tk.Tk()
            self.root.title("Bitget Market Data Visualization")
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
            
            # 윈도우 크기 및 위치 설정
            screen_width = self.root.winfo_screenwidth()
            screen_height = self.root.winfo_screenheight()
            window_width = 1200
            window_height = 800
            x = (screen_width - window_width) // 2
            y = (screen_height - window_height) // 2
            self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
            
            self._setup_gui()
            self.initialize_charts()
            
            # GUI 준비 완료 시그널
            self.gui_ready.set()
            
            # 업데이트 처리 시작
            self.root.after(100, self._process_update_queue)
            
            # GUI 이벤트 루프 시작
            self.root.mainloop()
            
        except Exception as e:
            logger.error(f"Error in GUI thread: {e}")
            self.gui_ready.set()
    
    def _setup_gui(self):
        """GUI 컴포넌트 설정"""
        # 메인 프레임
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 스타일 설정
        style = ttk.Style()
        style.configure('Custom.TFrame', background='#f0f0f0')
        
        # 차트 설정
        self.fig = Figure(figsize=(12, 8), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.main_frame)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        # 서브플롯 설정
        self.fig.subplots_adjust(hspace=0.3)
        self.ax1 = self.fig.add_subplot(211)
        self.ax2 = self.fig.add_subplot(212)
        
        # 정보 프레임
        self.info_frame = ttk.Frame(self.main_frame, style='Custom.TFrame')
        self.info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 레이블 스타일
        label_style = {'font': ('Helvetica', 10), 'padding': 5}
        
        # 레이블 추가
        self.correlation_label = ttk.Label(
            self.info_frame,
            text="상관계수: N/A",
            **label_style
        )
        self.correlation_label.pack(side=tk.LEFT, padx=5)
        
        self.price_label = ttk.Label(
            self.info_frame,
            text="현재가: N/A",
            **label_style
        )
        self.price_label.pack(side=tk.LEFT, padx=5)
        
        self.ratio_label = ttk.Label(
            self.info_frame,
            text="롱/숏 비율: N/A",
            **label_style
        )
        self.ratio_label.pack(side=tk.LEFT, padx=5)
        
        # 프로그레스 바
        self.progress = ttk.Progressbar(
            self.info_frame,
            mode='indeterminate',
            length=200
        )
        self.progress.pack(side=tk.RIGHT, padx=5)
    
    def initialize_charts(self):
        """차트 초기 설정"""
        # 캔들스틱 차트 설정
        self.ax1.set_title('BTCUSDT Price', pad=20, fontsize=12, fontweight='bold')
        self.ax1.grid(True, linestyle='--', alpha=0.7)
        self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        self.ax1.tick_params(axis='both', labelsize=10)
        
        # 롱숏 비율 차트 설정
        self.ax2.set_title('Long/Short Ratio', pad=20, fontsize=12, fontweight='bold')
        self.ax2.grid(True, linestyle='--', alpha=0.7)
        self.ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        self.ax2.tick_params(axis='both', labelsize=10)
        
        # 레이블 설정
        self.ax1.set_ylabel('Price (USDT)', fontsize=10)
        self.ax2.set_ylabel('Ratio (%)', fontsize=10)
        
        self.fig.tight_layout()
    
    def _process_update_queue(self):
        """업데이트 큐 처리"""
        try:
            current_time = time.time()
            if current_time - self.last_update >= self.update_interval:
                while not self.update_queue.empty():
                    update_data = self.update_queue.get_nowait()
                    self._do_update_charts(
                        update_data['candles'],
                        update_data['ratio_data']
                    )
                self.last_update = current_time
        except Exception as e:
            logger.error(f"Error processing update queue: {e}")
        finally:
            if not self.is_shutting_down:
                self.root.after(100, self._process_update_queue)
    
    def _auto_save_loop(self):
        """자동 저장 루프"""
        while self.auto_save and not self.is_shutting_down:
            try:
                current_time = time.time()
                elapsed_hours = (current_time - self.last_save_time) / 3600
                
                if elapsed_hours >= self.save_interval_hours:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = os.path.join(self.save_folder, f"market_chart_{timestamp}")
                    self.save_chart(filename)
                    
            except Exception as e:
                logger.error(f"Error in auto save loop: {e}")
            
            # 1분마다 체크
            time.sleep(60)
    
    def save_chart(self, filename: str, dpi: int = 300, format: str = 'png'):
        """차트를 고해상도 이미지로 저장"""
        try:
            with self.data_lock:
                current_time = datetime.now()
                hours_since_last_save = (time.time() - self.last_save_time) / 3600
                
                if self.is_shutting_down or hours_since_last_save >= self.save_interval_hours:
                    original_size = self.fig.get_size_inches()
                    self.fig.set_size_inches(24, 16)
                    
                    save_path = f"{filename}.{format}"
                    
                    self.fig.tight_layout()
                    self.fig.savefig(
                        save_path,
                        dpi=dpi,
                        format=format,
                        bbox_inches='tight',
                        pad_inches=0.5
                    )
                    
                    self.fig.set_size_inches(*original_size)
                    
                    width_pixels = int(24 * dpi)
                    height_pixels = int(16 * dpi)
                    
                    save_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"Saved chart at {save_time}: {save_path}")
                    logger.info(f"Image resolution: {width_pixels}x{height_pixels} pixels")
                    
                    self.last_save_time = time.time()
                    
        except Exception as e:
            logger.error(f"Error saving chart: {e}")
            if self.is_shutting_down:
                logger.error("Failed to save final chart during shutdown")
            raise
    
    def _do_update_charts(self, candles: List[Candle], ratio_data: List[Dict]):
        """차트 업데이트 수행"""
        try:
            self.progress.start()
            
            with self.data_lock:
                # 데이터 다운샘플링
                self.candle_data = self._downsample_data(candles, self.max_points)
                self.ratio_data = self._downsample_data(ratio_data, self.max_points)
                
                # 차트 업데이트
                self._update_candle_chart(self.candle_data)
                self._update_ratio_chart(self.ratio_data)
                
                # 상관관계 계산 및 표시
                correlation = self.calculate_correlation(self.candle_data, self.ratio_data)
                self.correlation_label.configure(text=f"상관계수: {correlation:.3f}")
                
                # 레이아웃 조정 및 그리기
                self.fig.tight_layout()
                self.canvas.draw_idle()
                
        except Exception as e:
            logger.error(f"Error updating charts: {e}")
        finally:
            self.progress.stop()
    
    def _downsample_data(self, data: list, max_points: int) -> list:
        """데이터 다운샘플링"""
        if len(data) <= max_points:
            return data
        indices = np.linspace(0, len(data) - 1, max_points, dtype=int)
        return [data[i] for i in indices]
    
    def _update_candle_chart(self, candles: List[Candle]):
        self.ax1.clear()
        self.ax1.grid(True, linestyle='--', alpha=0.7)
        
        if not candles:
            return
        
        # 데이터 준비
        timestamps = [datetime.fromtimestamp(c.timestamp/1000) for c in candles]
        opens = [c.open for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        closes = [c.close for c in candles]
        volumes = [c.volume for c in candles]
        
        # Y축 범위 설정
        min_price = min(lows)
        max_price = max(highs)
        price_margin = (max_price - min_price) * 0.1  # 10% 마진
        self.ax1.set_ylim(min_price - price_margin, max_price + price_margin)
        
        # 캔들스틱 그리기
        width = 0.0005
        colors = ['g' if close >= open else 'r' for open, close in zip(opens, closes)]
        
        self.ax1.bar(timestamps, np.array(highs) - np.array(lows),
                    bottom=lows, width=width, color=colors, alpha=0.8)
        self.ax1.bar(timestamps, np.array(closes) - np.array(opens),
                    bottom=opens, width=width*1.5, color=colors)
        
        # 거래량 차트
        volume_ax = self.ax1.twinx()
        volume_ax.bar(timestamps, volumes, width=width*2, alpha=0.3, color='gray')
        volume_ax.set_ylabel('Volume')
        
        # 차트 설정
        self.ax1.set_title('BTCUSDT Price', pad=20)
        self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        self.ax1.tick_params(axis='x', rotation=45)
        
        # Y축 포맷팅 - 천 단위 구분자 추가
        self.ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    def _update_ratio_chart(self, ratio_data: List[Dict]):
        """롱숏 비율 차트 업데이트"""
        self.ax2.clear()
        self.ax2.grid(True, linestyle='--', alpha=0.7)
        
        if not ratio_data:
            return
        
        # 데이터 준비
        ratio_times = [datetime.fromtimestamp(r['timestamp']/1000) for r in ratio_data]
        long_ratios = [r['long_ratio'] for r in ratio_data]
        short_ratios = [r['short_ratio'] for r in ratio_data]
        
        # 롱숏 비율 그래프 그리기
        self.ax2.plot(ratio_times, long_ratios, 'g-', label='Long', linewidth=2)
        self.ax2.plot(ratio_times, short_ratios, 'r-', label='Short', linewidth=2)
        self.ax2.fill_between(ratio_times, long_ratios, alpha=0.3, color='g')
        self.ax2.fill_between(ratio_times, short_ratios, alpha=0.3, color='r')
        
        # 차트 설정
        self.ax2.set_title('Long/Short Ratio', pad=20)
        self.ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        self.ax2.tick_params(axis='x', rotation=45)
        self.ax2.legend()
        
        # 최신 롱숏 비율 업데이트
        if ratio_data:
            latest_ratio = ratio_data[-1]
            long_short_ratio = latest_ratio['long_short_ratio']
            self.ratio_label.configure(text=f"롱/숏 비율: {long_short_ratio:.2f}")
    
    def calculate_correlation(self, candles: List[Candle], ratio_data: List[Dict]) -> float:
        """가격과 롱숏비율의 상관관계 계산"""
        try:
            if not candles or not ratio_data:
                return 0.0
            
            # 시계열 데이터 정렬
            candle_times = np.array([c.timestamp for c in candles])
            ratio_times = np.array([r['timestamp'] for r in ratio_data])
            
            # 같은 타임스탬프의 데이터만 사용
            common_times = np.intersect1d(candle_times, ratio_times)
            
            if len(common_times) < 2:
                return 0.0
            
            # 공통 시간대의 데이터 추출
            prices = []
            ratios = []
            
            for t in common_times:
                candle = next(c for c in candles if c.timestamp == t)
                ratio = next(r for r in ratio_data if r['timestamp'] == t)
                
                prices.append(candle.close)
                ratios.append(ratio['long_short_ratio'])
            
            # 상관계수 계산
            return float(np.corrcoef(prices, ratios)[0, 1])
            
        except Exception as e:
            logger.error(f"Error calculating correlation: {e}")
            return 0.0
    
    async def update_charts(self, candles: List[Candle], ratio_data: List[Dict]):
        """비동기 차트 업데이트"""
        try:
            if not self.is_shutting_down:
                self.update_queue.put({
                    'candles': candles,
                    'ratio_data': ratio_data
                })
        except Exception as e:
            logger.error(f"Error queuing chart update: {e}")
    
    def _on_closing(self):
        """GUI 종료 처리"""
        try:
            self.is_shutting_down = True
            logger.info("Saving final chart before shutdown...")
            
            # 마지막 차트 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.save_folder, f"market_chart_final_{timestamp}")
            self.save_chart(filename)
            
            # 자동 저장 중지
            self.auto_save = False
            
            # GUI 종료
            self.root.quit()
            self.root.destroy()
            
            logger.info("Application shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def close(self):
        """비동기 종료"""
        if hasattr(self, 'root') and self.root and not self.is_shutting_down:
            self.root.after(0, self._on_closing)
    
    def __del__(self):
        """소멸자"""
        if not self.is_shutting_down:
            try:
                # 마지막 저장 시도
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.join(self.save_folder, f"market_chart_final_{timestamp}")
                self.save_chart(filename)
            except:
                pass
            finally:
                self.auto_save = False