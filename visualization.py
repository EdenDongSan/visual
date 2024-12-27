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
import asyncio

logger = logging.getLogger(__name__)

class MarketDataVisualization:
    def __init__(self, max_points: int = 1000):
        self.max_points = max_points
        self.update_queue = queue.Queue()
        self.data_lock = threading.Lock()
        
        # Data stores
        self.price_data = []
        self.times_data = []
        self.ratio_data = []
        self.volume_data = []
        
        # GUI update control
        self.last_update = time.time()
        self.update_interval = 0.5  # Reduced to 500ms
        
        # Auto-save settings
        self.auto_save = True
        self.save_interval_hours = 2
        self.last_save_time = time.time()
        self.save_folder = "chart_images"
        self.is_shutting_down = False
        
        # Create save folder
        os.makedirs(self.save_folder, exist_ok=True)
        logger.info(f"Created chart save directory: {self.save_folder}")
        
        self._setup_gui_thread()
        self._start_auto_save()
    
    def _setup_gui_thread(self):
        """Set up and start GUI thread"""
        self.gui_thread = threading.Thread(target=self._run_gui, daemon=True)
        self.gui_ready = threading.Event()
        self.gui_thread.start()
        self.gui_ready.wait()
    
    def _start_auto_save(self):
        """Start auto-save thread"""
        save_thread = threading.Thread(target=self._auto_save_loop, daemon=True)
        save_thread.start()
        logger.info(f"Started auto-save with {self.save_interval_hours} hour interval")
    
    def _run_gui(self):
        """Main GUI thread loop"""
        try:
            self.root = tk.Tk()
            self.root.title("Bitget Market Data Visualization")
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
            
            # Window size and position
            screen_width = self.root.winfo_screenwidth()
            screen_height = self.root.winfo_screenheight()
            window_width = 1200
            window_height = 800
            x = (screen_width - window_width) // 2
            y = (screen_height - window_height) // 2
            self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
            
            self._setup_gui()
            self.initialize_chart()
            
            # GUI ready signal
            self.gui_ready.set()
            
            # Start update processing
            self.root.after(100, self._process_update_queue)
            
            # Start GUI event loop
            self.root.mainloop()
            
        except Exception as e:
            logger.error(f"Error in GUI thread: {e}")
            self.gui_ready.set()
    
    def _setup_gui(self):
        """Set up GUI components"""
        # Main frame
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Style setup
        style = ttk.Style()
        style.configure('Custom.TFrame', background='#f0f0f0')
        
        # Chart setup
        self.fig = Figure(figsize=(12, 8), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.main_frame)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        # Info frame
        self.info_frame = ttk.Frame(self.main_frame, style='Custom.TFrame')
        self.info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Label style
        label_style = {'font': ('Helvetica', 10), 'padding': 5}
        
        # Labels
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
        
        self.volume_label = ttk.Label(
            self.info_frame,
            text="거래량: N/A",
            **label_style
        )
        self.volume_label.pack(side=tk.LEFT, padx=5)
        
        # Progress bar
        self.progress = ttk.Progressbar(
            self.info_frame,
            mode='indeterminate',
            length=200
        )
        self.progress.pack(side=tk.RIGHT, padx=5)
    
    def initialize_chart(self):
        """Initialize chart layout"""
        self.ax1 = self.fig.add_subplot(111)  # Single chart
        
        # Price axis (left)
        self.ax1.set_ylabel('Price (USDT)', color='black')
        self.ax1.tick_params(axis='y', labelcolor='black')
        
        # Long/Short ratio axis (right)
        self.ax2 = self.ax1.twinx()
        self.ax2.set_ylabel('Long/Short Ratio (%)', color='blue')
        self.ax2.tick_params(axis='y', labelcolor='blue')
        
        # Volume axis (right, offset)
        self.ax3 = self.ax1.twinx()
        self.ax3.spines['right'].set_position(('outward', 60))
        self.ax3.set_ylabel('Volume', color='gray')
        self.ax3.tick_params(axis='y', labelcolor='gray')
        
        self.fig.tight_layout()
    
    def _process_update_queue(self):
        """Process update queue"""
        try:
            current_time = time.time()
            if current_time - self.last_update >= self.update_interval:
                while not self.update_queue.empty():
                    data = self.update_queue.get_nowait()
                    self._update_chart(data)
                self.last_update = current_time
        except Exception as e:
            logger.error(f"Error processing update queue: {e}")
        finally:
            if not self.is_shutting_down:
                self.root.after(100, self._process_update_queue)
    
    def _auto_save_loop(self):
        """Auto-save loop"""
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
            
            time.sleep(60)  # Check every minute
    
    def _update_chart(self, data):
        """Update chart with new data"""
        with self.data_lock:
            try:
                if 'candles' in data:
                    # Handle candles data
                    candles = data['candles']
                    if not candles:
                        return
                    
                    # Convert timestamps and extract data
                    self.times_data = [datetime.fromtimestamp(candle.timestamp/1000) for candle in candles]
                    self.price_data = [candle.close for candle in candles]
                    self.volume_data = [candle.volume for candle in candles]
                    
                    # Get latest values for labels
                    latest_price = self.price_data[-1] if self.price_data else 0
                    latest_volume = self.volume_data[-1] if self.volume_data else 0
                    
                    # Get ratio data if available
                    if 'ratios' in data:
                        ratios = data['ratios']
                        if ratios:
                            self.ratio_data = [ratio['long_short_ratio'] for ratio in ratios]
                            latest_ratio = self.ratio_data[-1] if self.ratio_data else 0
                            self.ratio_label.configure(text=f"롱/숏 비율: {latest_ratio:.2f}")
                    
                    # Maintain max points
                    if len(self.times_data) > self.max_points:
                        self.times_data = self.times_data[-self.max_points:]
                        self.price_data = self.price_data[-self.max_points:]
                        self.volume_data = self.volume_data[-self.max_points:]
                        if self.ratio_data:
                            self.ratio_data = self.ratio_data[-self.max_points:]
                    
                    # Clear charts
                    self.ax1.clear()
                    self.ax2.clear()
                    self.ax3.clear()
                    
                    # Draw price line
                    self.ax1.plot(self.times_data, self.price_data, 'black', linewidth=1.5)
                    
                    # Draw volume bars
                    self.ax3.bar(self.times_data, self.volume_data, alpha=0.3, color='gray')
                    
                    # Draw ratio line if available
                    if self.ratio_data and len(self.ratio_data) == len(self.times_data):
                        self.ax2.plot(self.times_data, self.ratio_data, 'blue', linewidth=1.5)
                    
                    # Set axes
                    self.ax1.set_xlabel('Time')
                    self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    self.ax1.tick_params(axis='x', rotation=45)
                    
                    # Update labels
                    self.price_label.configure(text=f"현재가: {latest_price:,.2f}")
                    self.volume_label.configure(text=f"거래량: {latest_volume:,.2f}")
                    
                    # Chart refresh
                    self.fig.tight_layout()
                    self.canvas.draw_idle()
                
            except Exception as e:
                logger.error(f"Error updating chart: {e}")
    
    def save_chart(self, filename: str, dpi: int = 300, format: str = 'png'):
        """Save chart as high-resolution image"""
        try:
            with self.data_lock:
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
                logger.info(f"Saved chart: {save_path}")
                self.last_save_time = time.time()
                
        except Exception as e:
            logger.error(f"Error saving chart: {e}")
            raise
    
    def calculate_trend_probabilities(self, candles: List[Candle], ratios: List[Dict], 
                                window_size: int = 20, trend_threshold: float = 0.02) -> Dict:
        """
        Calculate trend continuation probabilities based on price and ratio patterns.
        
        Parameters:
        - candles: List of Candle objects containing price data
        - ratios: List of dictionaries containing long/short ratios
        - window_size: Number of periods to consider for trend analysis
        - trend_threshold: Minimum price change to consider as trend (2% default)
        
        Returns:
        Dictionary containing probabilities for different scenarios
        """
        try:
            # Extract price and ratio data
            prices = np.array([candle.close for candle in candles])
            long_ratios = np.array([ratio['long_ratio'] for ratio in ratios])
            short_ratios = np.array([ratio['short_ratio'] for ratio in ratios])
            
            # Initialize results dictionary
            results = {
                'uptrend_long_increase': {'count': 0, 'continued': 0, 'probability': 0.0},
                'uptrend_short_increase': {'count': 0, 'continued': 0, 'probability': 0.0},
                'downtrend_long_increase': {'count': 0, 'continued': 0, 'probability': 0.0},
                'downtrend_short_increase': {'count': 0, 'continued': 0, 'probability': 0.0}
            }
            
            # Calculate rolling returns for trend identification
            for i in range(len(prices) - window_size * 2):  # Need extra window for future returns
                # Current window
                current_window = prices[i:i+window_size]
                current_return = (current_window[-1] / current_window[0]) - 1
                
                # Future window for continuation check
                future_window = prices[i+window_size:i+window_size*2]
                future_return = (future_window[-1] / future_window[0]) - 1
                
                # Ratio changes
                ratio_window = window_size // 2  # Shorter window for ratio analysis
                long_ratio_change = (long_ratios[i+window_size-1] / long_ratios[i+window_size-ratio_window]) - 1
                short_ratio_change = (short_ratios[i+window_size-1] / short_ratios[i+window_size-ratio_window]) - 1
                
                # Analyze uptrend patterns
                if current_return > trend_threshold:
                    # Long ratio increasing in uptrend
                    if long_ratio_change > 0:
                        results['uptrend_long_increase']['count'] += 1
                        if future_return > 0:  # Trend continued
                            results['uptrend_long_increase']['continued'] += 1
                    
                    # Short ratio increasing in uptrend
                    if short_ratio_change > 0:
                        results['uptrend_short_increase']['count'] += 1
                        if future_return > 0:
                            results['uptrend_short_increase']['continued'] += 1
                
                # Analyze downtrend patterns
                elif current_return < -trend_threshold:
                    # Long ratio increasing in downtrend
                    if long_ratio_change > 0:
                        results['downtrend_long_increase']['count'] += 1
                        if future_return < 0:  # Trend continued
                            results['downtrend_long_increase']['continued'] += 1
                    
                    # Short ratio increasing in downtrend
                    if short_ratio_change > 0:
                        results['downtrend_short_increase']['count'] += 1
                        if future_return < 0:
                            results['downtrend_short_increase']['continued'] += 1
            
            # Calculate probabilities
            for scenario in results:
                if results[scenario]['count'] > 0:
                    results[scenario]['probability'] = (
                        results[scenario]['continued'] / results[scenario]['count']
                    )
            
            # Add current market state analysis
            current_trend = self.analyze_current_market_state(prices, long_ratios, short_ratios, 
                                                            window_size, trend_threshold)
            results['current_market_state'] = current_trend
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating trend probabilities: {e}")
            return {}

    def analyze_current_market_state(self, prices: np.ndarray, long_ratios: np.ndarray, 
                                short_ratios: np.ndarray, window_size: int, 
                                trend_threshold: float) -> Dict:
        """
        Analyze the current market state and identify active patterns.
        """
        try:
            # Calculate current trend
            current_window = prices[-window_size:]
            current_return = (current_window[-1] / current_window[0]) - 1
            
            # Calculate recent ratio changes
            ratio_window = window_size // 2
            long_ratio_change = (long_ratios[-1] / long_ratios[-ratio_window]) - 1
            short_ratio_change = (short_ratios[-1] / short_ratios[-ratio_window]) - 1
            
            current_state = {
                'trend': 'neutral',
                'long_ratio_increasing': long_ratio_change > 0,
                'short_ratio_increasing': short_ratio_change > 0,
                'return': current_return,
                'warning_level': 0  # 0: normal, 1: attention, 2: high risk
            }
            
            # Classify current trend
            if current_return > trend_threshold:
                current_state['trend'] = 'uptrend'
                if short_ratio_change > 0.05:  # Significant short increase during uptrend
                    current_state['warning_level'] = 2
                elif long_ratio_change < -0.05:  # Significant long decrease during uptrend
                    current_state['warning_level'] = 1
                    
            elif current_return < -trend_threshold:
                current_state['trend'] = 'downtrend'
                if long_ratio_change > 0.05:  # Significant long increase during downtrend
                    current_state['warning_level'] = 2
                elif short_ratio_change < -0.05:  # Significant short decrease during downtrend
                    current_state['warning_level'] = 1
            
            return current_state
            
        except Exception as e:
            logger.error(f"Error analyzing current market state: {e}")
            return {}
        
    async def update_data(self, ticker_data):
        """Async data update"""
        try:
            self.update_queue.put(ticker_data)
        except Exception as e:
            logger.error(f"Error queuing data update: {e}")
    
    def _on_closing(self):
        """Handle GUI closure"""
        try:
            self.is_shutting_down = True
            logger.info("Saving final chart before shutdown...")
            
            # Save final chart
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.save_folder, f"market_chart_final_{timestamp}")
            self.save_chart(filename)
            
            # Stop auto-save
            self.auto_save = False
            
            # Close GUI
            self.root.quit()
            self.root.destroy()
            
            logger.info("Application shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def close(self):
        """Async closure"""
        if hasattr(self, 'root') and self.root and not self.is_shutting_down:
            # 메인 스레드에서 실행되도록 수정
            await asyncio.get_event_loop().run_in_executor(None, self._on_closing)
            # 종료 처리를 위한 대기
            await asyncio.sleep(0.5)
            
    def __del__(self):
        """Destructor"""
        if not self.is_shutting_down:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = os.path.join(self.save_folder, f"market_chart_final_{timestamp}")
                self.save_chart(filename)
            except:
                pass
            finally:
                self.auto_save = False