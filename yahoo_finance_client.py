"""
Yahoo Finance market data source implementation.
Uses the yfinance library for data access.
"""
import os
import logging
import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import yfinance as yf

from .data_source_interface import MarketDataSource

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yahoo_finance_client")

class YahooFinanceClient(MarketDataSource):
    """
    Client for interacting with Yahoo Finance API.
    Uses the yfinance library for data access.
    """
    
    def __init__(self):
        """Initialize the Yahoo Finance client"""
        # No API key needed
        pass
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "yahoo_finance"
        
    @property
    def daily_call_limit(self) -> Optional[int]:
        """Return the daily API call limit (None if unlimited)"""
        return None  # Yahoo Finance has no formal API limits
    
    async def get_current_price(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get current price for a single ticker
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Dictionary with price data or None if unavailable
        """
        try:
            # Use a separate thread for the yfinance API call
            loop = asyncio.get_event_loop()
            ticker_data = await loop.run_in_executor(
                None, 
                lambda: yf.Ticker(ticker)
            )
            
            # Get latest price data
            history = await loop.run_in_executor(
                None,
                lambda: ticker_data.history(period="1d")
            )
            
            if history.empty:
                logger.warning(f"No data available for {ticker}")
                return None
            
            # Get the latest price from history
            latest_price = history["Close"].iloc[-1]
            timestamp = history.index[-1]
            
            return {
                "price": float(latest_price),
                "timestamp": timestamp.to_pydatetime(),
                "volume": int(history["Volume"].iloc[-1]) if "Volume" in history else None,
                "source": self.source_name
            }
            
        except Exception as e:
            logger.error(f"Error getting data for {ticker}: {str(e)}")
            return None
    
    async def get_batch_prices(self, tickers: List[str], max_batch_size: int = 100) -> Dict[str, Dict[str, Any]]:
        """
        Get current prices for multiple tickers
        
        Args:
            tickers: List of ticker symbols
            max_batch_size: Maximum batch size for a single request
            
        Returns:
            Dictionary mapping tickers to their price data
        """
        if not tickers:
            return {}
            
        results = {}
        
        # Process in batches to avoid overloading
        for i in range(0, len(tickers), max_batch_size):
            batch = tickers[i:i+max_batch_size]
            batch_str = " ".join(batch)
            
            try:
                # Use a separate thread for the yfinance API call
                loop = asyncio.get_event_loop()
                
                # Download data for the batch
                data = await loop.run_in_executor(
                    None,
                    lambda: yf.download(batch_str, period="1d", group_by="ticker")
                )
                
                # Process results
                if len(batch) == 1:
                    # Handle single ticker case where data is not grouped
                    ticker = batch[0]
                    if not data.empty:
                        latest = data.iloc[-1]
                        results[ticker] = {
                            "price": float(latest["Close"]),
                            "timestamp": datetime.now(),
                            "volume": int(latest["Volume"]) if "Volume" in latest else None,
                            "source": self.source_name
                        }
                else:
                    # Handle multi-ticker case where data is grouped by ticker
                    for ticker in batch:
                        if ticker in data and not data[ticker].empty:
                            ticker_data = data[ticker]
                            latest = ticker_data.iloc[-1]
                            
                            # Ensure Close is available
                            if "Close" in latest:
                                close_price = latest["Close"]
                                # Handle the case where Close might be a Series
                                if hasattr(close_price, "iloc"):
                                    close_price = close_price.iloc[0]
                                
                                results[ticker] = {
                                    "price": float(close_price),
                                    "timestamp": datetime.now(),
                                    "volume": int(latest["Volume"]) if "Volume" in latest else None,
                                    "source": self.source_name
                                }
                
                # Add a short delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error in batch lookup for {len(batch)} tickers: {str(e)}")
                
                # If batch lookup fails, try individual lookups
                for ticker in batch:
                    try:
                        price_data = await self.get_current_price(ticker)
                        if price_data:
                            results[ticker] = price_data
                    except Exception as individual_error:
                        logger.error(f"Error getting data for {ticker}: {str(individual_error)}")
                    
                    # Add a short delay between individual requests
                    await asyncio.sleep(0.2)
        
        return results
    
    async def get_company_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get company metrics for a ticker (PE ratio, market cap, etc.)
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Dictionary with company metrics or None if unavailable
        """
        try:
            # Use a separate thread for the yfinance API call
            loop = asyncio.get_event_loop()
            ticker_data = await loop.run_in_executor(
                None, 
                lambda: yf.Ticker(ticker)
            )
            
            # Get company info
            info = await loop.run_in_executor(
                None,
                lambda: ticker_data.info
            )
            
            if not info:
                logger.warning(f"No company info available for {ticker}")
                return None
                
            # Extract relevant fields
            return {
                "company_name": info.get("shortName") or info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "dividend_yield": info.get("dividendYield"),
                "dividend_rate": info.get("dividendRate"),
                "eps": info.get("trailingEPS"),
                "avg_volume": info.get("averageVolume"),
                "source": self.source_name
            }
            
        except Exception as e:
            logger.error(f"Error getting company info for {ticker}: {str(e)}")
            return None
    
    async def get_historical_prices(self, ticker: str, start_date: datetime, end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get historical prices for a ticker
        
        Args:
            ticker: Ticker symbol
            start_date: Start date for historical data
            end_date: End date for historical data (defaults to today)
            
        Returns:
            List of historical price data points
        """
        try:
            # Use current date if end_date not provided
            if not end_date:
                end_date = datetime.now()
                
            # Convert dates to strings in the format required by yfinance
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            
            # Use a separate thread for the yfinance API call
            loop = asyncio.get_event_loop()
            history = await loop.run_in_executor(
                None,
                lambda: yf.download(ticker, start=start_str, end=end_str)
            )
            
            if history.empty:
                logger.warning(f"No historical data available for {ticker} from {start_str} to {end_str}")
                return []
            
            # Convert to list of dictionaries
            results = []
            for date, row in history.iterrows():
                results.append({
                    "date": date.date(),
                    "timestamp": date.to_pydatetime(),
                    "open": float(row["Open"]) if "Open" in row and not pd.isna(row["Open"]) else None,
                    "high": float(row["High"]) if "High" in row and not pd.isna(row["High"]) else None,
                    "low": float(row["Low"]) if "Low" in row and not pd.isna(row["Low"]) else None,
                    "close": float(row["Close"]) if "Close" in row and not pd.isna(row["Close"]) else None,
                    "volume": int(row["Volume"]) if "Volume" in row and not pd.isna(row["Volume"]) else None,
                    "source": self.source_name
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting historical data for {ticker}: {str(e)}")
            return []