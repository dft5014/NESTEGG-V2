"""
Enhanced price updater that uses multiple data sources and is decoupled from
portfolio calculations.
"""
import os
import logging
import asyncio
import databases
import sqlalchemy
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
from dotenv import load_dotenv

# Import our modules
from api_clients.market_data_manager import MarketDataManager
from utils.common import record_system_event, update_system_event
from redis_cache import FastCache

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price_updater_v2")

# Initialize Database Connection
DATABASE_URL = os.getenv("DATABASE_URL")
database = databases.Database(DATABASE_URL)

class PriceUpdaterV2:
    """
    Enhanced price updater that uses multiple data sources.
    This is decoupled from portfolio calculations.
    """
    
    def __init__(self):
        """Initialize the price updater with necessary clients"""
        self.database = database
        self.market_data = MarketDataManager()
    
    async def connect(self):
        """Connect to the database"""
        if not self.database.is_connected:
            await self.database.connect()
    
    async def disconnect(self):
        """Disconnect from the database"""
        if self.database.is_connected:
            await self.database.disconnect()
    
    async def get_active_tickers(self) -> List[str]:
        """
        Get all active tickers from the database
        
        Returns:
            List of active ticker symbols
        """
        query = """
            SELECT ticker 
            FROM securities 
            WHERE active = true
        """
        
        result = await self.database.fetch_all(query)
        return [row['ticker'] for row in result]
    
    async def mark_ticker_unavailable(self, ticker: str) -> None:
        """
        Mark a ticker as unavailable
        
        Args:
            ticker: Ticker symbol to mark as unavailable
        """
        query = """
            UPDATE securities 
            SET on_yfinance = false
            WHERE ticker = :ticker
        """
        
        await self.database.execute(query, {"ticker": ticker})
        logger.warning(f"Marked ticker {ticker} as unavailable")
    
    async def update_security_prices(self, tickers=None, max_tickers=None) -> Dict[str, Any]:
        """
        Update current prices for securities using multiple data sources
        
        Args:
            tickers: Optional list of specific tickers to update
            max_tickers: Maximum number of tickers to update (for testing)
            
        Returns:
            Summary of updates made
        """
        try:
            await self.connect()
            
            # Record the start of this operation
            event_id = await record_system_event(
                self.database, 
                "price_update", 
                "started", 
                {"source": "multiple", "tickers": tickers}
            )
            
            # Start timing
            start_time = datetime.now()
            
            # Get active tickers
            if tickers:
                # If specific tickers provided, validate they exist in the database
                placeholders = ', '.join([f"'{ticker}'" for ticker in tickers])
                query = f"""
                    SELECT ticker 
                    FROM securities 
                    WHERE ticker IN ({placeholders})
                """
                result = await self.database.fetch_all(query)
                all_tickers = [row['ticker'] for row in result]
                
                # Check if any requested tickers don't exist
                missing_tickers = set(tickers) - set(all_tickers)
                if missing_tickers:
                    logger.warning(f"Tickers not found in database: {missing_tickers}")
            else:
                # Otherwise get all active tickers
                all_tickers = await self.get_active_tickers()
            
            # Apply max_tickers limit if specified
            if max_tickers and len(all_tickers) > max_tickers:
                selected_tickers = all_tickers[:max_tickers]
            else:
                selected_tickers = all_tickers
                
            logger.info(f"Updating prices for {len(selected_tickers)} securities")
            
            # Get price data from market data manager (handles multiple sources)
            price_data = await self.market_data.get_batch_prices(selected_tickers)
            
            # Track statistics
            update_count = 0
            unavailable_count = 0
            sources_used = set()
            price_updates = {}
            processed_tickers = set()
            
            # Update securities table with new prices
            for ticker, data in price_data.items():
                try:
                    # Track which sources were used
                    if "source" in data:
                        sources_used.add(data["source"])
                    
                    # Update the security record
                    await self.database.execute(
                        """
                        UPDATE securities 
                        SET 
                            current_price = :price, 
                            last_updated = :timestamp,
                            data_source = :source
                        WHERE ticker = :ticker
                        """,
                        {
                            "ticker": ticker,
                            "price": data["price"],
                            "timestamp": datetime.utcnow(),
                            "source": data.get("source", "unknown")
                        }
                    )
                    
                    # Add to price history
                    await self.database.execute(
                        """
                        INSERT INTO price_history 
                        (ticker, close_price, timestamp, date, source)
                        VALUES (:ticker, :price, :timestamp, :date, :source)
                        ON CONFLICT (ticker, date) DO UPDATE
                        SET close_price = :price, timestamp = :timestamp, source = :source
                        """,
                        {
                            "ticker": ticker,
                            "price": data["price"],
                            "timestamp": datetime.utcnow(),
                            "date": datetime.utcnow().date(),
                            "source": data.get("source", "unknown")
                        }
                    )
                    
                    # Store update information for response
                    price_updates[ticker] = {
                        "price": data["price"],
                        "source": data.get("source", "unknown"),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    processed_tickers.add(ticker)
                    update_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating security {ticker}: {str(e)}")
            
            # Check for tickers that failed to get data
            missing_tickers = set(selected_tickers) - processed_tickers
            
            # Mark tickers as unavailable if they failed to get data
            for ticker in missing_tickers:
                await self.mark_ticker_unavailable(ticker)
                unavailable_count += 1
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record completion
            result = {
                "total_tickers": len(selected_tickers),
                "updated_count": update_count,
                "unavailable_count": unavailable_count,
                "sources_used": list(sources_used),
                "price_updates": price_updates,
                "duration_seconds": duration
            }
            
            await update_system_event(
                self.database,
                event_id,
                "completed",
                result
            )
            
            # After successful update, invalidate relevant caches
            if FastCache.is_available():
                # Invalidate cached portfolio calculations
                FastCache.delete_pattern("portfolio:*")
                
                # Invalidate cached security data for processed tickers
                for ticker in processed_tickers:
                    FastCache.delete(f"security:{ticker}")
                    FastCache.delete(f"security_history:{ticker}*")
                    
                # Invalidate securities list
                FastCache.delete("securities:all")
                
                logger.info(f"Invalidated cache for {len(processed_tickers)} securities")
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating security prices: {str(e)}")
            
            # Record failure
            if event_id:
                await update_system_event(
                    self.database,
                    event_id,
                    "failed",
                    {"error": str(e)},
                    str(e)
                )
            
            raise
        finally:
            await self.disconnect()
    
    async def update_company_metrics(self, tickers=None, max_tickers=None) -> Dict[str, Any]:
        """
        Update company metrics for securities
        
        Args:
            tickers: Optional list of specific tickers to update
            max_tickers: Maximum number of tickers to update (for testing)
            
        Returns:
            Summary of updates made
        """
        try:
            await self.connect()
            
            # Record the start of this operation
            event_id = await record_system_event(
                self.database, 
                "metrics_update", 
                "started", 
                {"tickers": tickers}
            )
            
            # Start timing
            start_time = datetime.now()
            
            # Get active tickers
            if tickers:
                # If specific tickers provided, validate they exist in the database
                placeholders = ', '.join([f"'{ticker}'" for ticker in tickers])
                query = f"""
                    SELECT ticker 
                    FROM securities 
                    WHERE ticker IN ({placeholders})
                """
                result = await self.database.fetch_all(query)
                all_tickers = [row['ticker'] for row in result]
                
                # Check if any requested tickers don't exist
                missing_tickers = set(tickers) - set(all_tickers)
                if missing_tickers:
                    logger.warning(f"Tickers not found in database: {missing_tickers}")
            else:
                # Otherwise get all active tickers
                all_tickers = await self.get_active_tickers()
            
            # Apply max_tickers limit if specified
            if max_tickers and len(all_tickers) > max_tickers:
                selected_tickers = all_tickers[:max_tickers]
            else:
                selected_tickers = all_tickers
                
            logger.info(f"Updating metrics for {len(selected_tickers)} securities")
            
            # Track statistics
            update_count = 0
            unavailable_count = 0
            sources_used = set()
            metrics_updates = {}
            updated_tickers = set()
            
            # Process each ticker individually
            for ticker in selected_tickers:
                try:
                    # Get company metrics
                    metrics = await self.market_data.get_company_metrics(ticker)
                    
                    if not metrics:
                        logger.warning(f"No metrics available for {ticker}")
                        unavailable_count += 1
                        continue
                    
                    # Track which sources were used
                    if "source" in metrics:
                        sources_used.add(metrics["source"])
                    
                    # Update the security record
                    await self.database.execute(
                        """
                        UPDATE securities 
                        SET 
                            company_name = :company_name,
                            sector = :sector,
                            industry = :industry,
                            market_cap = :market_cap,
                            pe_ratio = :pe_ratio,
                            dividend_yield = :dividend_yield,
                            dividend_rate = :dividend_rate,
                            eps = :eps,
                            avg_volume = :avg_volume,
                            last_metrics_update = :timestamp,
                            metrics_source = :source
                        WHERE ticker = :ticker
                        """,
                        {
                            "ticker": ticker,
                            "company_name": metrics.get("company_name"),
                            "sector": metrics.get("sector"),
                            "industry": metrics.get("industry"),
                            "market_cap": metrics.get("market_cap"),
                            "pe_ratio": metrics.get("pe_ratio"),
                            "dividend_yield": metrics.get("dividend_yield"),
                            "dividend_rate": metrics.get("dividend_rate"),
                            "eps": metrics.get("eps"),
                            "avg_volume": metrics.get("avg_volume"),
                            "timestamp": datetime.utcnow(),
                            "source": metrics.get("source", "unknown")
                        }
                    )
                    
                    # Store metrics information for response
                    metrics_updates[ticker] = {
                        "company_name": metrics.get("company_name"),
                        "sector": metrics.get("sector"),
                        "industry": metrics.get("industry"),
                        "source": metrics.get("source", "unknown"),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    updated_tickers.add(ticker)
                    update_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating metrics for {ticker}: {str(e)}")
                    unavailable_count += 1
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record completion
            result = {
                "total_tickers": len(selected_tickers),
                "updated_count": update_count,
                "unavailable_count": unavailable_count,
                "sources_used": list(sources_used),
                "metrics_updates": metrics_updates,
                "duration_seconds": duration
            }
            
            await update_system_event(
                self.database,
                event_id,
                "completed",
                result
            )
            
            # After successful update, invalidate relevant caches
            if FastCache.is_available():
                # Invalidate cached security data for updated tickers
                for ticker in updated_tickers:
                    FastCache.delete(f"security:{ticker}")
                    
                # Invalidate securities list
                FastCache.delete("securities:all")
                
                logger.info(f"Invalidated cache for {len(updated_tickers)} securities")
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating company metrics: {str(e)}")
            
            # Record failure
            if event_id:
                await update_system_event(
                    self.database,
                    event_id,
                    "failed",
                    {"error": str(e)},
                    str(e)
                )
            
            raise
        finally:
            await self.disconnect()
    
    async def update_historical_prices(self, tickers=None, max_tickers=None, days=30) -> Dict[str, Any]:
        """
        Update historical prices for securities
        
        Args:
            tickers: Optional list of specific tickers to update
            max_tickers: Maximum number of tickers to update (for testing)
            days: Number of days of history to fetch
            
        Returns:
            Summary of updates made
        """
        try:
            await self.connect()
            
            # Record the start of this operation
            event_id = await record_system_event(
                self.database, 
                "history_update", 
                "started", 
                {"days": days, "tickers": tickers}
            )
            
            # Start timing
            start_time = datetime.now()
            
            # Get tickers to update
            if tickers:
                # If specific tickers provided, validate they exist in the database
                placeholders = ', '.join([f"'{ticker}'" for ticker in tickers])
                query = f"""
                    SELECT ticker 
                    FROM securities 
                    WHERE ticker IN ({placeholders})
                """
                result = await self.database.fetch_all(query)
                all_tickers = [row['ticker'] for row in result]
                
                # Check if any requested tickers don't exist
                missing_tickers = set(tickers) - set(all_tickers)
                if missing_tickers:
                    logger.warning(f"Tickers not found in database: {missing_tickers}")
            else:
                # Otherwise get all active tickers
                all_tickers = await self.get_active_tickers()
            
            # Apply max_tickers limit if specified
            if max_tickers and len(all_tickers) > max_tickers:
                selected_tickers = all_tickers[:max_tickers]
            else:
                selected_tickers = all_tickers
                
            logger.info(f"Updating historical prices for {len(selected_tickers)} securities ({days} days)")
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Track statistics
            update_count = 0
            unavailable_count = 0
            price_points_added = 0
            sources_used = set()
            history_updates = {}
            updated_tickers = set()
            
            # Process each ticker individually
            for ticker in selected_tickers:
                try:
                    # Get historical prices
                    historical_data = await self.market_data.get_historical_prices(ticker, start_date, end_date)
                    
                    if not historical_data:
                        logger.warning(f"No historical data available for {ticker}")
                        unavailable_count += 1
                        continue
                    
                    # Track the source that was used
                    for point in historical_data:
                        if "source" in point:
                            sources_used.add(point["source"])
                            break
                    
                    ticker_points = 0
                    # Process each data point
                    for point in historical_data:
                        try:
                            # Insert or update price history record
                            await self.database.execute(
                                """
                                INSERT INTO price_history 
                                (ticker, close_price, open_price, high_price, low_price, volume, timestamp, date, source)
                                VALUES (:ticker, :close, :open, :high, :low, :volume, :timestamp, :date, :source)
                                ON CONFLICT (ticker, date) DO UPDATE
                                SET 
                                    close_price = :close,
                                    open_price = :open,
                                    high_price = :high,
                                    low_price = :low,
                                    volume = :volume,
                                    timestamp = :timestamp,
                                    source = :source
                                """,
                                {
                                    "ticker": ticker,
                                    "close": point.get("close"),
                                    "open": point.get("open"),
                                    "high": point.get("high"),
                                    "low": point.get("low"),
                                    "volume": point.get("volume"),
                                    "timestamp": point.get("timestamp") or datetime.utcnow(),
                                    "date": point.get("date"),
                                    "source": point.get("source", "unknown")
                                }
                            )
                            
                            price_points_added += 1
                            ticker_points += 1
                            
                        except Exception as point_error:
                            logger.error(f"Error adding historical price for {ticker} on {point.get('date')}: {str(point_error)}")
                    
                    # Store summary for this ticker
                    history_updates[ticker] = {
                        "points_added": ticker_points,
                        "date_range": {
                            "start": start_date.isoformat(),
                            "end": end_date.isoformat()
                        }
                    }
                    
                    # Update security's last_backfilled timestamp
                    await self.database.execute(
                        """
                        UPDATE securities 
                        SET last_backfilled = :timestamp
                        WHERE ticker = :ticker
                        """,
                        {
                            "ticker": ticker,
                            "timestamp": datetime.utcnow()
                        }
                    )
                    
                    updated_tickers.add(ticker)
                    update_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating historical prices for {ticker}: {str(e)}")
                    unavailable_count += 1
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record completion
            result = {
                "total_tickers": len(selected_tickers),
                "updated_count": update_count,
                "unavailable_count": unavailable_count,
                "price_points_added": price_points_added,
                "sources_used": list(sources_used),
                "history_updates": history_updates,
                "duration_seconds": duration
            }
            
            await update_system_event(
                self.database,
                event_id,
                "completed",
                result
            )
            
            # After successful update, invalidate relevant caches
            if FastCache.is_available():
                # Invalidate security history caches
                for ticker in updated_tickers:
                    FastCache.delete(f"security_history:{ticker}*")
                
                logger.info(f"Invalidated historical data cache for {len(updated_tickers)} securities")
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating historical prices: {str(e)}")
            
            # Record failure
            if event_id:
                await update_system_event(
                    self.database,
                    event_id,
                    "failed",
                    {"error": str(e)},
                    str(e)
                )
            
            raise
        finally:
            await self.disconnect()

    async def smart_update(self, update_type="all", max_tickers=None) -> Dict[str, Any]:
        """
        Perform a smart update of security data based on what needs updating most
        
        Args:
            update_type: Type of update to perform (all, prices, metrics, history)
            max_tickers: Maximum number of tickers to update per operation
            
        Returns:
            Summary of updates made
        """
        try:
            await self.connect()
            
            start_time = datetime.now()
            
            # Record the start of this operation
            event_id = await record_system_event(
                self.database, 
                "smart_update", 
                "started", 
                {"update_type": update_type, "max_tickers": max_tickers}
            )
            
            results = {}
            
            # Determine which tickers need price updates most urgently
            if update_type in ["all", "prices"]:
                # Find tickers with oldest price updates
                price_query = """
                SELECT ticker
                FROM securities
                WHERE active = true AND on_yfinance = true
                ORDER BY COALESCE(last_updated, '1970-01-01') ASC
                LIMIT :limit
                """
                
                price_tickers = await self.database.fetch_all(
                    price_query, 
                    {"limit": max_tickers or 100}
                )
                
                if price_tickers:
                    price_tickers_list = [row["ticker"] for row in price_tickers]
                    logger.info(f"Smart update: Updating prices for {len(price_tickers_list)} securities")
                    results["prices"] = await self.update_security_prices(
                        tickers=price_tickers_list
                    )
            
            # Determine which tickers need metrics updates most urgently
            if update_type in ["all", "metrics"]:
                # Find tickers with oldest metrics updates
                metrics_query = """
                SELECT ticker
                FROM securities
                WHERE active = true AND on_yfinance = true
                ORDER BY COALESCE(last_metrics_update, '1970-01-01') ASC
                LIMIT :limit
                """
                
                metrics_tickers = await self.database.fetch_all(
                    metrics_query, 
                    {"limit": max_tickers or 50}  # Fewer tickers for metrics as it's slower
                )
                
                if metrics_tickers:
                    metrics_tickers_list = [row["ticker"] for row in metrics_tickers]
                    logger.info(f"Smart update: Updating metrics for {len(metrics_tickers_list)} securities")
                    results["metrics"] = await self.update_company_metrics(
                        tickers=metrics_tickers_list
                    )
            
            # Determine which tickers need historical data updates most urgently
            if update_type in ["all", "history"]:
                # Find tickers with oldest historical updates
                history_query = """
                SELECT ticker
                FROM securities
                WHERE active = true AND on_yfinance = true
                ORDER BY COALESCE(last_backfilled, '1970-01-01') ASC
                LIMIT :limit
                """
                
                history_tickers = await self.database.fetch_all(
                    history_query, 
                    {"limit": max_tickers or 20}  # Even fewer tickers for history as it's most intensive
                )
                
                if history_tickers:
                    history_tickers_list = [row["ticker"] for row in history_tickers]
                    logger.info(f"Smart update: Updating historical data for {len(history_tickers_list)} securities")
                    results["history"] = await self.update_historical_prices(
                        tickers=history_tickers_list,
                        days=30  # Default to 30 days of history
                    )
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Create summary result
            summary = {
                "update_type": update_type,
                "duration_seconds": duration,
                "results": results
            }
            
            # Compute overall statistics
            total_updated = 0
            total_unavailable = 0
            all_sources_used = set()
            
            for key, result in results.items():
                if "updated_count" in result:
                    total_updated += result["updated_count"]
                if "unavailable_count" in result:
                    total_unavailable += result["unavailable_count"]
                if "sources_used" in result:
                    all_sources_used.update(result["sources_used"])
            
            summary["total_updated"] = total_updated
            summary["total_unavailable"] = total_unavailable
            summary["all_sources_used"] = list(all_sources_used)
            
            # Record completion
            await update_system_event(
                self.database,
                event_id,
                "completed",
                summary
            )
            
            return summary
            
        except Exception as e:
            logger.error(f"Error in smart update: {str(e)}")
            
            # Record failure
            if event_id:
                await update_system_event(
                    self.database,
                    event_id,
                    "failed",
                    {"error": str(e)},
                    str(e)
                )
            
            raise
        finally:
            await self.disconnect()

# Standalone execution function
async def run_price_update(update_type: str = "prices", max_tickers: int = None):
    """
    Run the price updater as a standalone script
    
    Args:
        update_type: Type of update to perform (prices, metrics, history, smart)
        max_tickers: Maximum number of tickers to process
    """
    updater = PriceUpdaterV2()
    try:
        result = None
        
        if update_type == "prices":
            result = await updater.update_security_prices(max_tickers=max_tickers)
            print(f"Price update complete: {result}")
        elif update_type == "metrics":
            result = await updater.update_company_metrics(max_tickers=max_tickers)
            print(f"Metrics update complete: {result}")
        elif update_type == "history":
            # Default to 30 days of history
            result = await updater.update_historical_prices(max_tickers=max_tickers, days=30)
            print(f"Historical price update complete: {result}")
        elif update_type == "smart":
            # Smart update that prioritizes what needs updating most
            result = await updater.smart_update(update_type="all", max_tickers=max_tickers)
            print(f"Smart update complete: {result}")
        else:
            print(f"Unknown update type: {update_type}")
            
    except Exception as e:
        print(f"Update failed: {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="NestEgg Market Data Updater")
    parser.add_argument("--type", choices=["prices", "metrics", "history", "smart"], default="smart", help="Type of update to perform")
    parser.add_argument("--max", type=int, help="Maximum number of tickers to process")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers to update")
    
    args = parser.parse_args()
    
    # Handle tickers argument
    tickers_list = None
    if args.tickers:
        tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')]
    
    asyncio.run(run_price_update(args.type, args.max))