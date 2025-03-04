"""
Polygon.io market data source implementation.
"""
import os
import logging
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from .data_source_interface import MarketDataSource

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("polygon_client")

class PolygonClient(MarketDataSource):
    """
    Client for interacting with Polygon.io API.
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the Polygon client with API key
        
        Args:
            api_key: Polygon API key (optional, will use environment variable if not provided)
        """
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("Polygon API key not provided and not found in environment")
            
        self.base_url = "https://api.polygon.io"
    
    @property
    def source_name(self) -> str:
        """Return the name of this data source"""
        return "polygon"
        
    @property
    def daily_call_limit(self) -> Optional[int]:
        """Return the daily API call limit (None if unlimited)"""
        return 5  # Free tier limit - update this if using a paid plan
    
    async def get_current_price(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get current price for a single ticker
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Dictionary with price data or None if unavailable
        """
        try:
            url = f"{self.base_url}/v2/last/trade/{ticker}"
            params = {"apiKey": self.api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to get price for {ticker}: Status {response.status}")
                        return None
                        
                    data = await response.json()
                    
                    # Check if we have valid data
                    if data.get("status") != "success" or "results" not in data:
                        logger.warning(f"Invalid data for {ticker}: {data}")
                        return None
                        
                    trade = data["results"]
                    
                    return {
                        "price": float(trade["p"]),  # price
                        "timestamp": datetime.fromtimestamp(trade["t"] / 1000),  # timestamp (ms to datetime)
                        "volume": int(trade["s"]) if "s" in trade else None,  # size (volume)
                        "source": self.source_name
                    }
        
        except Exception as e:
            logger.error(f"Error getting price for {ticker} from Polygon: {str(e)}")
            return None
    
    async def get_batch_prices(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get current prices for multiple tickers
        Note: Polygon free tier doesn't support batch lookups efficiently, so we call individually
        
        Args:
            tickers: List of ticker symbols
            
        Returns:
            Dictionary mapping tickers to their price data
        """
        results = {}
        
        # Process each ticker individually
        for ticker in tickers:
            price_data = await self.get_current_price(ticker)
            if price_data:
                results[ticker] = price_data
            
            # Add a short delay to avoid rate limiting
            await asyncio.sleep(0.2)
        
        return results
    
    async def get_company_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get company metrics for a ticker
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Dictionary with company metrics or None if unavailable
        """
        try:
            # For Polygon, we need to make a few separate API calls to get all the metrics
            # First, get ticker details
            url = f"{self.base_url}/v3/reference/tickers/{ticker}"
            params = {"apiKey": self.api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to get ticker details for {ticker}: Status {response.status}")
                        return None
                        
                    data = await response.json()
                    
                    # Check if we have valid data
                    if data.get("status") != "OK" or "results" not in data:
                        logger.warning(f"Invalid ticker details for {ticker}: {data}")
                        return None
                        
                    ticker_details = data["results"]
                    
            # Get financials for additional metrics
            url = f"{self.base_url}/v2/reference/financials/{ticker}"
            params = {
                "apiKey": self.api_key,
                "limit": 1  # Just get the most recent report
            }
            
            financials = None
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("status") == "OK" and "results" in data and data["results"]:
                            financials = data["results"][0]
                    
            # Combine the data
            metrics = {
                "company_name": ticker_details.get("name"),
                "sector": ticker_details.get("sic_description"),
                "market_cap": ticker_details.get("market_cap"),
                "source": self.source_name
            }
            
            # Add financial metrics if available
            if financials:
                metrics.update({
                    "pe_ratio": financials.get("ratios", {}).get("priceToEarningsRatio"),
                    "dividend_yield": financials.get("ratios", {}).get("dividendYield"),
                    "eps": financials.get("ratios", {}).get("earningsPerBasicShare")
                })
                
            return metrics
                
        except Exception as e:
            logger.error(f"Error getting company metrics for {ticker} from Polygon: {str(e)}")
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
                
            # Convert dates to Unix timestamps in milliseconds
            start_ms = int(start_date.timestamp() * 1000)
            end_ms = int(end_date.timestamp() * 1000)
            
            # Build the URL for aggregates endpoint
            url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_ms}/{end_ms}"
            params = {"apiKey": self.api_key, "sort": "asc"}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to get historical data for {ticker}: Status {response.status}")
                        return []
                        
                    data = await response.json()
                    
                    # Check if we have valid data
                    if data.get("status") != "OK" or "results" not in data:
                        logger.warning(f"Invalid historical data for {ticker}: {data}")
                        return []
                        
                    results = []
                    for bar in data["results"]:
                        results.append({
                            "date": datetime.fromtimestamp(bar["t"] / 1000).date(),
                            "timestamp": datetime.fromtimestamp(bar["t"] / 1000),
                            "open": float(bar["o"]),
                            "high": float(bar["h"]),
                            "low": float(bar["l"]),
                            "close": float(bar["c"]),
                            "volume": int(bar["v"]),
                            "source": self.source_name
                        })
                    
                    return results
                    
        except Exception as e:
            logger.error(f"Error getting historical data for {ticker} from Polygon: {str(e)}")
            return []