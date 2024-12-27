import asyncio
import logging
from dotenv import load_dotenv
import os
import sys
from market_data import MarketDataManager
from visualization import MarketDataVisualization
import signal
import threading

# Load environment variables
load_dotenv()

# Configure logging
def setup_logging():
    """Configure logging with rotating file handler"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('market_data.log')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class Application:
    def __init__(self):
        self.market_data = MarketDataManager()
        self.visualization = MarketDataVisualization(max_points=1000)
        self.update_interval = 1.0
        self.should_run = True
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.should_run = False
    
    async def update_visualization(self, data):
        """Visualization update callback with error handling"""
        try:
            if not self.should_run:
                return
                
            recent_data = self.market_data.get_recent_data()
            await self.visualization.update_charts(
                candles=recent_data['candles'],
                ratio_data=recent_data['ratios']
            )
            
            correlation = self.visualization.calculate_correlation(
                recent_data['candles'],
                recent_data['ratios']
            )
            logger.info(f"Price-Ratio Correlation: {correlation:.3f}")
            
        except Exception as e:
            logger.error(f"Error updating visualization: {e}")
    
    async def run(self):
        """Main application loop with improved error handling"""
        try:
            # Register callback
            self.market_data.add_callback(self.update_visualization)
            
            # Start market data collection
            market_data_task = asyncio.create_task(self.market_data.run())
            
            # Wait for shutdown signal
            while self.should_run:
                await asyncio.sleep(0.1)
            
            # Cleanup
            logger.info("Shutting down application...")
            self.market_data.stop()
            await self.visualization.close()
            
            # Cancel market data task
            market_data_task.cancel()
            try:
                await market_data_task
            except asyncio.CancelledError:
                pass
            
        except Exception as e:
            logger.error(f"Application error: {e}")
        finally:
            # Ensure cleanup
            self.market_data.stop()
            await self.visualization.close()

async def main():
    """Application entry point with error handling"""
    try:
        # Verify environment variables
        required_env_vars = [
            'MYSQL_HOST',
            'MYSQL_USER',
            'MYSQL_PASSWORD',
            'MYSQL_DATABASE',
            'BITGET_API_KEY',
            'BITGET_SECRET_KEY',
            'BITGET_PASSPHRASE'
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            return
        
        app = Application()
        await app.run()
        
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Fatal application error: {e}")
    finally:
        # Cleanup any remaining tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Stop event loop
        loop = asyncio.get_event_loop()
        loop.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        # Ensure event loop is closed
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()
        except Exception as e:
            logger.error(f"Error closing event loop: {e}")