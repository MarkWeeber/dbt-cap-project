#!/usr/bin/env python3
"""
DuckDB Load Script for dbt Capstone Project
============================================
Loads parquet files from data/raw into DuckDB RAW schema.
Part of the ELT pipeline: Extract → Load → Transform (dbt)

This script:
1. Connects to DuckDB (creates if not exists)
2. Creates RAW schema
3. Discovers all parquet files recursively in data/raw
4. Creates appropriate tables (BTCUSDT_H1, BTCUSDT_D1, etc.)
5. Loads all parquet data into respective tables

Author: Mark Weeber
Date: 2026-10-08
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

import duckdb
import pandas as pd

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# Path to DuckDB database file
# From scripts/ folder, go up one level to project root
DUCKDB_PATH = Path("../dev.duckdb").resolve()

# Path to raw data directory (parquet files)
# From scripts/ folder, go up one level then into data/raw
RAW_DATA_PATH = Path("../data/raw").resolve()

# Schema name for raw data
RAW_SCHEMA = "RAW"

# Expected table name pattern: SYMBOL_INTERVAL (e.g., BTCUSDT_H1)
# This regex extracts symbol and interval from filenames like BTCUSDT_H1.parquet
TABLE_NAME_PATTERN = re.compile(r'^([A-Z]+USDT)_(H1|D1|1h|1d)\.parquet$', re.IGNORECASE)

# Logging configuration
LOG_LEVEL = logging.INFO

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Configure logging for the load process.
    """
    logger = logging.getLogger("duckdb_loader")
    logger.setLevel(LOG_LEVEL)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ParquetFile:
    """
    Represents a discovered parquet file with its metadata.
    """
    path: Path
    table_name: str
    symbol: str
    interval: str
    
    def __repr__(self) -> str:
        return f"ParquetFile({self.path.name} -> {self.table_name})"

# =============================================================================
# DISCOVERY FUNCTIONS
# =============================================================================

def discover_parquet_files(data_path: Path) -> List[ParquetFile]:
    """
    Recursively discover all parquet files in the data directory.
    
    Walks through all subdirectories (month folders like 2024_01) and
    identifies valid parquet files matching the expected naming pattern.
    
    Args:
        data_path: Root path to data/raw directory
        
    Returns:
        List of ParquetFile objects with parsed metadata
        
    Raises:
        FileNotFoundError: If data_path doesn't exist
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path}")
    
    parquet_files: List[ParquetFile] = []
    
    # Recursively find all .parquet files
    # rglob pattern **/*.parquet searches all subdirectories
    for parquet_path in data_path.rglob("*.parquet"):
        # Parse filename to extract symbol and interval
        match = TABLE_NAME_PATTERN.match(parquet_path.name)
        
        if match:
            symbol = match.group(1).upper()  # BTCUSDT
            interval = match.group(2).upper()  # H1 or D1
            
            # Normalize interval naming (1h -> H1, 1d -> D1 for consistency)
            if interval == "1H":
                interval = "H1"
            elif interval == "1D":
                interval = "D1"
            
            table_name = f"{symbol}_{interval}"
            
            parquet_files.append(ParquetFile(
                path=parquet_path,
                table_name=table_name,
                symbol=symbol,
                interval=interval
            ))
            logger.debug(f"Discovered: {parquet_path} -> {table_name}")
        else:
            logger.warning(f"Skipping file with unexpected name format: {parquet_path.name}")
    
    # Sort by path for consistent ordering
    parquet_files.sort(key=lambda x: str(x.path))
    
    logger.info(f"Discovered {len(parquet_files)} parquet files in {data_path}")
    return parquet_files

def group_files_by_table(parquet_files: List[ParquetFile]) -> Dict[str, List[ParquetFile]]:
    """
    Group parquet files by their target table name.
    
    Multiple files (from different months) can belong to the same table.
    For example: 2024_01/BTCUSDT_H1.parquet and 2024_02/BTCUSDT_H1.parquet
    both go into the BTCUSDT_H1 table.
    
    Args:
        parquet_files: List of discovered parquet files
        
    Returns:
        Dictionary mapping table_name -> list of ParquetFile objects
    """
    grouped: Dict[str, List[ParquetFile]] = defaultdict(list)
    
    for pf in parquet_files:
        grouped[pf.table_name].append(pf)
    
    # Log summary
    logger.info(f"Files grouped into {len(grouped)} tables:")
    for table_name, files in sorted(grouped.items()):
        logger.info(f"  {table_name}: {len(files)} file(s)")
        for f in files:
            logger.debug(f"    - {f.path}")
    
    return dict(grouped)

# =============================================================================
# DUCKDB OPERATIONS
# =============================================================================

class DuckDBLoader:
    """
    Manages DuckDB connection and data loading operations.
    """
    
    def __init__(self, db_path: Path):
        """
        Initialize DuckDB loader.
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        
    def connect(self) -> None:
        """
        Establish connection to DuckDB database.
        Creates the database file if it doesn't exist.
        """
        try:
            logger.info(f"Connecting to DuckDB: {self.db_path}")
            
            # Ensure parent directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Connect to database (creates if not exists)
            self.conn = duckdb.connect(str(self.db_path))
            
            # Install and load parquet extension (usually built-in, but ensure it's loaded)
            self.conn.execute("INSTALL parquet;")
            self.conn.execute("LOAD parquet;")
            
            logger.info("Successfully connected to DuckDB")
            
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")
    
    def create_schema(self) -> None:
        """
        Create RAW schema if it doesn't exist.
        """
        try:
            logger.info(f"Creating schema '{RAW_SCHEMA}' if not exists")
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
            logger.info(f"Schema '{RAW_SCHEMA}' is ready")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def get_existing_tables(self) -> Set[str]:
        """
        Get set of existing table names in the RAW schema.
        
        Returns:
            Set of table names (uppercase for comparison)
        """
        try:
            result = self.conn.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{RAW_SCHEMA}'
            """).fetchall()
            
            tables = {row[0].upper() for row in result}
            logger.info(f"Found {len(tables)} existing tables in {RAW_SCHEMA} schema")
            return tables
            
        except Exception as e:
            logger.warning(f"Could not retrieve existing tables: {e}")
            return set()
    
    def drop_table_if_exists(self, table_name: str) -> None:
        """
        Drop a table if it exists (for full refresh).
        
        Args:
            table_name: Name of table to drop
        """
        try:
            full_name = f"{RAW_SCHEMA}.{table_name}"
            self.conn.execute(f"DROP TABLE IF EXISTS {full_name}")
            logger.info(f"Dropped existing table: {full_name}")
        except Exception as e:
            logger.warning(f"Could not drop table {table_name}: {e}")
    
    def create_table_from_parquet(self, table_name: str, parquet_files: List[ParquetFile]) -> int:
        """
        Create table and load data from parquet files.
        
        Uses DuckDB's ability to read multiple parquet files efficiently.
        
        Args:
            table_name: Name of table to create
            parquet_files: List of parquet files to load
            
        Returns:
            Number of rows loaded
        """
        if not parquet_files:
            logger.warning(f"No files to load for table {table_name}")
            return 0
        
        full_table_name = f"{RAW_SCHEMA}.{table_name}"
        
        try:
            # Convert paths to strings for SQL
            # Use absolute paths to avoid any working directory issues
            file_paths = [str(f.path.absolute()) for f in parquet_files]
            
            logger.info(f"Loading {len(file_paths)} file(s) into {full_table_name}")
            
            # For multiple files, use read_parquet with list
            # DuckDB handles this efficiently
            if len(file_paths) == 1:
                # Single file - simpler query
                files_sql = f"'{file_paths[0]}'"
            else:
                # Multiple files - use list syntax
                files_sql = "[" + ", ".join(f"'{p}'" for p in file_paths) + "]"
            
            # Drop existing table for idempotent loading (full refresh)
            self.drop_table_if_exists(table_name)
            
            # Create table from parquet files
            # DuckDB automatically infers schema from parquet
            create_sql = f"""
                CREATE TABLE {full_table_name} AS 
                SELECT * FROM read_parquet({files_sql})
            """
            
            logger.debug(f"Executing: {create_sql[:100]}...")
            self.conn.execute(create_sql)
            
            # Get row count
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()
            row_count = count_result[0] if count_result else 0
            
            # Get column info for logging
            columns_result = self.conn.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{RAW_SCHEMA}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()
            
            logger.info(f"Created table {full_table_name} with {row_count} rows")
            logger.info(f"Columns: {', '.join(f'{col[0]}({col[1]})' for col in columns_result)}")
            
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to create/load table {table_name}: {e}")
            raise
    
    def verify_table_data(self, table_name: str) -> Dict:
        """
        Verify loaded data by returning basic statistics.
        
        Args:
            table_name: Name of table to verify
            
        Returns:
            Dictionary with statistics
        """
        full_name = f"{RAW_SCHEMA}.{table_name}"
        
        try:
            # Get row count
            count = self.conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
            
            # Get date range if Timestamp column exists
            date_range = None
            try:
                date_result = self.conn.execute(f"""
                    SELECT 
                        MIN(Timestamp) as min_date,
                        MAX(Timestamp) as max_date
                    FROM {full_name}
                """).fetchone()
                date_range = (date_result[0], date_result[1])
            except:
                pass
            
            return {
                "table": table_name,
                "row_count": count,
                "date_range": date_range
            }
            
        except Exception as e:
            logger.warning(f"Could not verify table {table_name}: {e}")
            return {"table": table_name, "error": str(e)}
    
    def get_summary(self) -> pd.DataFrame:
        """
        Get summary of all tables in RAW schema.
        
        Returns:
            DataFrame with table statistics
        """
        try:
            result = self.conn.execute(f"""
                SELECT 
                    table_name,
                    (SELECT COUNT(*) FROM information_schema.columns 
                     WHERE table_schema = '{RAW_SCHEMA}' 
                     AND table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_schema = '{RAW_SCHEMA}'
                ORDER BY table_name
            """).fetchdf()
            
            # Add row counts
            row_counts = []
            for table in result['table_name']:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}").fetchone()[0]
                    row_counts.append(count)
                except:
                    row_counts.append(0)
            
            result['row_count'] = row_counts
            return result
            
        except Exception as e:
            logger.error(f"Could not generate summary: {e}")
            return pd.DataFrame()

# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def validate_environment() -> None:
    """
    Validate that the script is being run from the correct location
    and that required paths exist.
    """
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent
    
    logger.info(f"Script directory: {script_dir}")
    logger.info(f"Project root: {project_root}")
    
    # Check if data directory exists
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Raw data directory not found: {RAW_DATA_PATH}\n"
            f"Please run extract script first to download data."
        )
    
    logger.info(f"Raw data path: {RAW_DATA_PATH}")

def main() -> int:
    """
    Main execution function.
    
    Returns:
        0 on success, 1 on failure
    """
    logger.info("=" * 60)
    logger.info("DuckDB Load Process Starting")
    logger.info("=" * 60)
    
    try:
        # Step 1: Validate environment
        validate_environment()
        
        # Step 2: Discover parquet files
        parquet_files = discover_parquet_files(RAW_DATA_PATH)
        
        if not parquet_files:
            logger.error("No parquet files found. Exiting.")
            return 1
        
        # Step 3: Group files by target table
        files_by_table = group_files_by_table(parquet_files)
        
        # Step 4: Connect to DuckDB and load data
        loader = DuckDBLoader(DUCKDB_PATH)
        
        with loader:  # Context manager ensures connection closes
            loader.connect()
            loader.create_schema()
            
            # Load each table
            total_rows = 0
            for table_name in sorted(files_by_table.keys()):
                files = files_by_table[table_name]
                row_count = loader.create_table_from_parquet(table_name, files)
                total_rows += row_count
            
            # Step 5: Verify and summarize
            logger.info("\n" + "=" * 60)
            logger.info("Load Summary")
            logger.info("=" * 60)
            
            summary = loader.get_summary()
            if not summary.empty:
                print("\n" + summary.to_string(index=False))
                logger.info(f"\nTotal rows loaded: {total_rows}")
                logger.info(f"Tables created: {len(summary)}")
            else:
                logger.warning("Could not generate summary")
        
        logger.info("\n" + "=" * 60)
        logger.info("Load process completed successfully")
        logger.info("=" * 60)
        logger.info(f"DuckDB database: {DUCKDB_PATH}")
        logger.info(f"Schema: {RAW_SCHEMA}")
        logger.info("You can now run dbt models with: dbt run")
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"Environment error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1

# Context manager support for DuckDBLoader
def __enter__(self):
    return self

def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()

# Patch the class
DuckDBLoader.__enter__ = __enter__
DuckDBLoader.__exit__ = __exit__

if __name__ == "__main__":
    sys.exit(main())