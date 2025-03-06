import yfinance as yf
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yfinance_debug")

async def debug_yfinance(ticker='AAPL'):
    try:
        logger.info(f"Debugging yfinance for ticker: {ticker}")
        
        # Check library version
        logger.info(f"YFinance Version: {yf.__version__}")
        
        # Fetch ticker data
        logger.info("Fetching ticker data...")
        ticker_obj = yf.Ticker(ticker)
        
        # Get basic info
        logger.info("Retrieving company info...")
        info = ticker_obj.info
        
        logger.info("Company Information:")
        for key, value in info.items():
            print(f"{key}: {value}")
        
        # Get historical data
        logger.info("Fetching historical prices...")
        history = ticker_obj.history(period="1d")
        
        logger.info("Latest Price Information:")
        if not history.empty:
            latest = history.iloc[-1]
            print(f"Close Price: {latest['Close']}")
            print(f"Volume: {latest['Volume']}")
        else:
            logger.warning("No historical price data available")
        
    except Exception as e:
        logger.error(f"Error in yfinance debugging: {e}")

# Run the async function
async def main():
    await debug_yfinance()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())