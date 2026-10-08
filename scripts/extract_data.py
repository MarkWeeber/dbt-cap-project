#!/usr/bin/env python3
"""
Binance OHLCV Data Downloader
=============================
Downloads historical OHLCV (Open, High, Low, Close, Volume) data from Binance
and saves it as Parquet files with monthly folder organization.

Author: Mark Weeber
Date: 2026-10-08
"""

import os
import sys
import time
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import requests
import pandas as pd
from dataclasses import dataclass

# =============================================================================
# CONFIGURATION SECTION - Modify these values as needed
# =============================================================================

# Base directory for saving data files
# Using Path for cross-platform compatibility (works on Windows, Linux, Mac)
# This will be created automatically if it doesn't exist
SAVE_PATH = Path("../data/raw")

# Trading pairs to download (must be valid Binance symbols)
SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# Time intervals to download (must be valid Binance intervals)
# Valid intervals: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
INTERVALS = ["1h", "1d"]  # Note: Binance uses lowercase, but we'll handle both

# DATE RANGE CONFIGURATION
# Supports multiple formats:
#   - "2024"          → interpreted as 2024-01-01
#   - "2024-03"       → interpreted as 2024-03-01  
#   - "2024-03-15"    → interpreted as 2024-03-15
#   - "" (empty)      → special handling (see below)
#
# For DATE_FROM: Empty string not allowed (must specify start)
# For DATE_TO: Empty string means "end of last completed month"
#              (e.g., if today is 2026-10-08, it becomes 2026-09-30)
DATE_FROM = "2026-08"  # Start date
DATE_TO = "2026-09"           # End date (empty = until end of last completed month)

# Rate limiting configuration
# Binance has a weight-based rate limit system. Each endpoint has a weight,
# and we must stay under the limit (typically 1200 weight per minute for raw requests)
RATE_LIMIT_CONFIG = {
    "max_requests_per_minute": 1200,  # Conservative limit
    "request_weight": 1,              # Weight per klines request
    "sleep_between_requests": 0.05,   # Seconds to sleep between requests (prevents hitting limits)
    "max_retries": 3,                 # Number of retries on failure
    "retry_delay": 5,                 # Seconds to wait between retries
}

# Data fetching configuration
FETCH_CONFIG = {
    "max_candles_per_request": 1000,  # Binance API hard limit
    "base_url": "https://api.binance.com",  # Spot API endpoint
    # Alternative: "https://fapi.binance.com" for USD-M Futures
}

# =============================================================================
# LOGGING SETUP - Helps with debugging and monitoring
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Configure logging to show both to console and optionally to file.
    This helps track progress and debug issues.
    """
    logger = logging.getLogger("binance_downloader")
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers to avoid duplicates if re-run
    logger.handlers = []
    
    # Console handler with readable format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# =============================================================================
# DATE PARSING UTILITIES
# =============================================================================

def parse_flexible_date(date_str: str, is_end_date: bool = False) -> datetime:
    """
    Parse date string in various formats and return a datetime object.
    
    Supported formats:
        - "2024"        → 2024-01-01 (or 2024-12-31 if is_end_date)
        - "2024-03"     → 2024-03-01 (or 2024-03-31 if is_end_date)
        - "2024-03-15"  → 2024-03-15 (or 2024-03-15 23:59:59 if is_end_date)
    
    Args:
        date_str: The date string to parse
        is_end_date: If True, adjust to end of period (last day of month/year)
    
    Returns:
        datetime object in UTC
        
    Raises:
        ValueError: If date string cannot be parsed
    """
    if not date_str or date_str.strip() == "":
        raise ValueError("Date string cannot be empty")
    
    date_str = date_str.strip()
    
    # Try to match different patterns
    patterns = [
        # Full date: 2024-03-15 or 2024/03/15
        (r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', 'ymd'),
        # Year-month: 2024-03 or 2024/03
        (r'^(\d{4})[-/](\d{1,2})$', 'ym'),
        # Year only: 2024
        (r'^(\d{4})$', 'y'),
    ]
    
    for pattern, dtype in patterns:
        match = re.match(pattern, date_str)
        if match:
            try:
                if dtype == 'ymd':
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    if is_end_date:
                        # Set to end of day
                        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    return dt
                    
                elif dtype == 'ym':
                    year, month = int(match.group(1)), int(match.group(2))
                    day = 1
                    if is_end_date:
                        # Last day of month
                        if month == 12:
                            day = 31
                        else:
                            # Get first day of next month and subtract one day
                            next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
                            from datetime import timedelta
                            last_day = next_month - timedelta(days=1)
                            day = last_day.day
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    if is_end_date:
                        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    return dt
                    
                elif dtype == 'y':
                    year = int(match.group(1))
                    month = 1
                    day = 1
                    if is_end_date:
                        month = 12
                        day = 31
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    if is_end_date:
                        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    return dt
                    
            except ValueError as e:
                raise ValueError(f"Invalid date values in '{date_str}': {e}")
    
    raise ValueError(f"Unable to parse date string: '{date_str}'. "
                     f"Supported formats: YYYY, YYYY-MM, YYYY-MM-DD")

def get_date_range() -> Tuple[datetime, datetime]:
    """
    Calculate the effective date range based on configuration.
    
    DATE_FROM: Parsed as start of period (first day)
    DATE_TO: 
        - If empty: End of last completed month (not current month)
        - If specified: Parsed as end of period (last day)
    
    Returns:
        Tuple of (start_datetime, end_datetime) in UTC
    """
    # Parse start date (always specified)
    try:
        start_dt = parse_flexible_date(DATE_FROM, is_end_date=False)
    except ValueError as e:
        logger.error(f"Invalid DATE_FROM: {e}")
        raise
    
    # Handle end date
    if DATE_TO and DATE_TO.strip():
        # Parse specified end date
        try:
            end_dt = parse_flexible_date(DATE_TO, is_end_date=True)
        except ValueError as e:
            logger.error(f"Invalid DATE_TO: {e}")
            raise
    else:
        # Empty DATE_TO: Use end of last completed month
        # Current date is 2026-10-08, so last completed month is September 2026
        now = datetime.now(timezone.utc)
        
        if now.month == 1:
            # If January, last completed month is December of previous year
            end_dt = datetime(now.year - 1, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        else:
            # Last day of previous month
            from datetime import timedelta
            # First day of current month
            first_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            # Subtract one day to get last day of previous month
            last_day = first_of_month - timedelta(days=1)
            end_dt = last_day.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logger.info(f"DATE_TO is empty - using end of last completed month: {end_dt.strftime('%Y-%m-%d')}")
    
    # Validate: start must be before end
    if start_dt > end_dt:
        raise ValueError(f"DATE_FROM ({start_dt}) must be before DATE_TO ({end_dt})")
    
    logger.info(f"Date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    
    return start_dt, end_dt

# =============================================================================
# DATA CLASSES - Type-safe configuration containers
# =============================================================================

@dataclass
class DownloadTask:
    """
    Represents a single download task with all necessary metadata.
    Using dataclass makes the code cleaner and type-safe.
    """
    symbol: str
    interval: str
    year: int
    month: int
    
    @property
    def folder_name(self) -> str:
        """Generate folder name in format 'YYYY_MM'"""
        return f"{self.year}_{self.month:02d}"
    
    @property
    def file_name(self) -> str:
        """Generate file name in format 'SYMBOL_INTERVAL.parquet'"""
        return f"{self.symbol}_{self.interval.upper()}.parquet"
    
    @property
    def full_path(self) -> Path:
        """Generate full path to the parquet file"""
        return SAVE_PATH / self.folder_name / self.file_name
    
    @property
    def start_time(self) -> int:
        """Get Unix timestamp (ms) for start of month"""
        dt = datetime(self.year, self.month, 1, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    
    @property
    def end_time(self) -> int:
        """Get Unix timestamp (ms) for end of month"""
        # Calculate first day of next month, then subtract 1ms
        if self.month == 12:
            next_month = datetime(self.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            next_month = datetime(self.year, self.month + 1, 1, tzinfo=timezone.utc)
        return int(next_month.timestamp() * 1000) - 1

# =============================================================================
# DIRECTORY MANAGEMENT
# =============================================================================

def ensure_directory_exists(path: Path) -> None:
    """
    Check if directory exists, create it (and all parent directories) if not.
    Uses exist_ok=True to prevent race conditions in parallel execution.
    
    This function creates the entire directory tree if needed, similar to 'mkdir -p'.
    """
    try:
        if not path.exists():
            logger.info(f"Creating directory: {path}")
            # parents=True creates all parent directories if they don't exist
            # exist_ok=True prevents errors if directory is created by another process
            path.mkdir(parents=True, exist_ok=True)
        else:
            logger.debug(f"Directory already exists: {path}")
            
        # Verify we can write to this directory
        if not os.access(path, os.W_OK):
            raise PermissionError(f"No write permission for directory: {path}")
            
    except PermissionError as e:
        logger.error(f"Permission denied when creating/accessing directory: {e}")
        raise
    except OSError as e:
        logger.error(f"OS error when creating directory: {e}")
        raise

def check_file_exists(task: DownloadTask) -> bool:
    """
    Check if a parquet file already exists for this task.
    Returns True if file exists and has content (size > 0).
    """
    file_path = task.full_path
    if file_path.exists() and file_path.stat().st_size > 0:
        logger.info(f"File already exists: {file_path}")
        return True
    return False

# =============================================================================
# BINANCE API INTERACTION
# =============================================================================

class BinanceAPIClient:
    """
    Handles all interactions with the Binance API.
    Includes rate limiting, retry logic, and error handling.
    """
    
    def __init__(self):
        self.base_url = FETCH_CONFIG["base_url"]
        self.session = requests.Session()  # Reuse connections for performance
        self.last_request_time = 0
        
    def _rate_limit(self) -> None:
        """
        Implement rate limiting to avoid hitting Binance API limits.
        Waits if necessary between requests.
        """
        min_interval = RATE_LIMIT_CONFIG["sleep_between_requests"]
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[List]:
        """
        Make a GET request to Binance API with retry logic.
        Returns the JSON response or None if all retries failed.
        """
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(RATE_LIMIT_CONFIG["max_retries"]):
            try:
                # Apply rate limiting before request
                self._rate_limit()
                
                logger.debug(f"Requesting: {url} with params {params}")
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for HTTP errors (4xx, 5xx)
                response.raise_for_status()
                
                # Return parsed JSON
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                # Handle specific Binance error codes
                if response.status_code == 429:
                    logger.warning("Rate limit hit! Waiting 60 seconds...")
                    time.sleep(60)
                elif response.status_code == 418:
                    logger.error("IP banned by Binance! Stopping.")
                    raise
                else:
                    logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
                
            # Wait before retry (exponential backoff)
            if attempt < RATE_LIMIT_CONFIG["max_retries"] - 1:
                delay = RATE_LIMIT_CONFIG["retry_delay"] * (2 ** attempt)
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
        
        logger.error(f"All {RATE_LIMIT_CONFIG['max_retries']} attempts failed")
        return None
    
    def get_klines(self, symbol: str, interval: str, 
                   start_time: int, end_time: int, 
                   limit: int = 1000) -> Optional[List[List]]:
        """
        Fetch kline/candlestick data from Binance API.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            interval: Time interval (e.g., "1h", "1d")
            start_time: Start time in milliseconds (Unix timestamp)
            end_time: End time in milliseconds (Unix timestamp)
            limit: Maximum number of candles to return (max 1000)
            
        Returns:
            List of klines or None if request failed
            Each kline contains: [open_time, open, high, low, close, volume, 
                                  close_time, quote_volume, trades, 
                                  taker_buy_volume, taker_buy_quote_volume, ignore]
        """
        endpoint = "/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval.lower(),  # Binance uses lowercase
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        
        return self._make_request(endpoint, params)
    
    def get_server_time(self) -> Optional[int]:
        """
        Get current server time from Binance.
        Useful for checking API connectivity.
        """
        response = self._make_request("/api/v3/time", {})
        if response:
            return response.get("serverTime")
        return None

# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_klines_to_dataframe(klines: List[List]) -> pd.DataFrame:
    """
    Convert raw Binance kline data to a properly formatted DataFrame.
    
    Binance kline format:
    [
        1499040000000,      // Open time (timestamp in ms)
        "0.01634790",       // Open price
        "0.80000000",       // High price
        "0.01575800",       // Low price
        "0.01577100",       // Close price
        "148976.11427815",  // Volume
        1499644799999,      // Close time
        "2434.19055334",    // Quote asset volume
        308,                // Number of trades
        "1756.87402397",    // Taker buy base asset volume
        "28.46694368",      // Taker buy quote asset volume
        "17928899.62484339" // Ignore
    ]
    """
    if not klines:
        return pd.DataFrame()
    
    # Create DataFrame with all columns from Binance
    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades',
        'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    
    # Select and rename only the columns we need
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']].copy()
    df.columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    
    # Convert data types
    # Timestamp from milliseconds to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
    
    # Price and volume columns to float
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Sort by timestamp (ascending)
    df = df.sort_values('Timestamp').reset_index(drop=True)
    
    return df

def fetch_monthly_data(client: BinanceAPIClient, task: DownloadTask) -> pd.DataFrame:
    """
    Fetch all OHLCV data for a specific month, handling pagination.
    Since Binance returns max 1000 candles per request, we may need multiple requests.
    """
    all_klines = []
    start_time = task.start_time
    end_time = task.end_time
    
    logger.info(f"Fetching {task.symbol} {task.interval} for {task.folder_name}")
    
    while start_time < end_time:
        # Fetch batch of data
        klines = client.get_klines(
            symbol=task.symbol,
            interval=task.interval,
            start_time=start_time,
            end_time=end_time,
            limit=FETCH_CONFIG["max_candles_per_request"]
        )
        
        if not klines:
            logger.warning(f"No data returned for range {start_time} to {end_time}")
            break
        
        all_klines.extend(klines)
        
        # Get timestamp of last candle to continue pagination
        last_candle_time = klines[-1][0]  # open_time of last candle
        
        # If we got less than max, we've reached the end
        if len(klines) < FETCH_CONFIG["max_candles_per_request"]:
            break
            
        # Move start time to after the last candle we received
        # Add 1ms to avoid overlap
        start_time = last_candle_time + 1
        
        logger.debug(f"Fetched {len(klines)} candles, continuing from {start_time}")
    
    logger.info(f"Total candles fetched: {len(all_klines)}")
    
    return process_klines_to_dataframe(all_klines)

# =============================================================================
# FILE OPERATIONS
# =============================================================================

def save_to_parquet(df: pd.DataFrame, task: DownloadTask) -> bool:
    """
    Save DataFrame to parquet file with specified path structure.
    Creates parent directories if they don't exist.
    
    Args:
        df: DataFrame to save
        task: DownloadTask containing path information
        
    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        logger.warning(f"No data to save for {task.file_name}")
        return False
    
    try:
        # Ensure the monthly folder exists
        month_folder = SAVE_PATH / task.folder_name
        ensure_directory_exists(month_folder)
        
        # Full path to file
        file_path = task.full_path
        
        # Save as parquet with compression (snappy is fast, good for time series)
        # Use index=False since we have Timestamp as a column
        df.to_parquet(
            file_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        logger.info(f"Successfully saved {len(df)} rows to {file_path}")
        
        # Log file size for monitoring
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to save parquet file: {e}")
        return False

# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def generate_download_tasks(start_dt: datetime, end_dt: datetime) -> List[DownloadTask]:
    """
    Generate all download tasks based on configuration and date range.
    Creates tasks for each symbol/interval/month combination within the date range.
    
    Args:
        start_dt: Start datetime (inclusive)
        end_dt: End datetime (inclusive)
        
    Returns:
        List of DownloadTask objects
    """
    tasks = []
    
    # Generate all year-month combinations in range
    current_year, current_month = start_dt.year, start_dt.month
    end_year, end_month = end_dt.year, end_dt.month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                # Normalize interval to lowercase for internal use
                interval_clean = interval.lower()
                
                tasks.append(DownloadTask(
                    symbol=symbol,
                    interval=interval_clean,
                    year=current_year,
                    month=current_month
                ))
        
        # Move to next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1
    
    logger.info(f"Generated {len(tasks)} download tasks")
    return tasks

def main():
    """
    Main execution function.
    Orchestrates the entire download process.
    """
    logger.info("=" * 60)
    logger.info("Binance OHLCV Data Downloader Starting")
    logger.info("=" * 60)
    
    # Step 1: Parse date range
    try:
        start_dt, end_dt = get_date_range()
    except ValueError as e:
        logger.error(f"Date configuration error: {e}")
        return 1
    
    # Step 2: Ensure base directory exists
    try:
        ensure_directory_exists(SAVE_PATH)
        logger.info(f"Data will be saved to: {SAVE_PATH.absolute()}")
    except (PermissionError, OSError) as e:
        logger.error(f"Cannot create/access save directory: {e}")
        return 1
    
    # Step 3: Initialize API client
    client = BinanceAPIClient()
    
    # Test connectivity
    server_time = client.get_server_time()
    if server_time:
        server_dt = datetime.fromtimestamp(server_time / 1000, tz=timezone.utc)
        logger.info(f"Connected to Binance API. Server time: {server_dt}")
    else:
        logger.error("Failed to connect to Binance API. Exiting.")
        return 1
    
    # Step 4: Generate download tasks
    tasks = generate_download_tasks(start_dt, end_dt)
    
    # Step 5: Process each task
    successful = 0
    skipped = 0
    failed = 0
    
    for i, task in enumerate(tasks, 1):
        logger.info(f"\n[{i}/{len(tasks)}] Processing: {task.symbol} {task.interval} {task.folder_name}")
        
        # Check if already downloaded
        if check_file_exists(task):
            logger.info("Skipping - already exists")
            skipped += 1
            continue
        
        try:
            # Fetch data
            df = fetch_monthly_data(client, task)
            
            if df.empty:
                logger.warning("No data available for this period")
                skipped += 1
                continue
            
            # Validate data
            logger.info(f"Data range: {df['Timestamp'].min()} to {df['Timestamp'].max()}")
            logger.info(f"Data shape: {df.shape}")
            
            # Save to parquet
            if save_to_parquet(df, task):
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            logger.error(f"Error processing task: {e}")
            failed += 1
            continue
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Download Summary")
    logger.info("=" * 60)
    logger.info(f"Total tasks: {len(tasks)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Skipped (exists/no data): {skipped}")
    logger.info(f"Failed: {failed}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())