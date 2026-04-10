"""
Partitioned storage utilities for scalable data management
Supports JSON and Parquet with automatic partitioning
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
from .config import config
from .logger import setup_logger

logger = setup_logger(__name__)


class PartitionedStorage:
    """Manage partitioned data storage with Parquet and JSON support"""
    
    def __init__(self, zone: str):
        """
        Initialize partitioned storage for a zone
        
        Args:
            zone: Storage zone (raw, bronze, silver, gold, analytics)
        """
        self.zone = zone
        self.base_path = config.get_storage_path(zone)
        self.format = config.get(f'storage.{zone}.format', 'parquet')
    
    def write(
        self, 
        data: Any, 
        run_date: datetime,
        filename: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Path:
        """
        Write data to partitioned storage
        
        Args:
            data: Data to write (DataFrame, dict, or list)
            run_date: Run date for partitioning
            filename: Optional filename (auto-generated if not provided)
            metadata: Optional metadata to include
        
        Returns:
            Path where data was written
        """
        # Generate partition path
        partition_path = config.get_partition_path(self.zone, run_date)
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if self.format == 'json':
                filename = f"data_{timestamp}.json"
            else:
                filename = f"data.parquet"
        
        file_path = partition_path / filename
        
        # Write data based on format
        if self.format == 'json':
            self._write_json(data, file_path, metadata)
        elif self.format == 'parquet':
            self._write_parquet(data, file_path)
        
        logger.info(f"Wrote data to {file_path}")
        return file_path
    
    def _write_json(self, data: Any, path: Path, metadata: Optional[Dict] = None):
        """Write data as JSON"""
        output = data if metadata is None else {**metadata, 'data': data}
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, default=str)
    
    def _write_parquet(self, data: pd.DataFrame, path: Path):
        """Write DataFrame as Parquet"""
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Parquet format requires pandas DataFrame")
        
        data.to_parquet(
            path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
    
    def read_partition(
        self, 
        run_date: datetime,
        filename: Optional[str] = None
    ) -> Any:
        """
        Read data from a specific partition
        
        Args:
            run_date: Run date of partition to read
            filename: Specific filename (reads all if not provided)
        
        Returns:
            Data (DataFrame for parquet, dict/list for JSON)
        """
        partition_path = config.get_partition_path(self.zone, run_date)
        
        if not partition_path.exists():
            raise FileNotFoundError(f"Partition not found: {partition_path}")
        
        if filename:
            file_path = partition_path / filename
            return self._read_file(file_path)
        else:
            # Read all files in partition
            return self._read_partition_all(partition_path)
    
    def _read_file(self, path: Path) -> Any:
        """Read a single file"""
        if path.suffix == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        elif path.suffix == '.parquet':
            return pd.read_parquet(path)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")
    
    def _read_partition_all(self, partition_path: Path) -> pd.DataFrame:
        """Read all files in a partition"""
        if self.format == 'parquet':
            parquet_files = list(partition_path.glob('*.parquet'))
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files in {partition_path}")
            
            dfs = [pd.read_parquet(f) for f in parquet_files]
            return pd.concat(dfs, ignore_index=True)
            
        elif self.format == 'json':
            json_files = list(partition_path.glob('*.json'))
            if not json_files:
                raise FileNotFoundError(f"No json files in {partition_path}")
            
            all_records = []
            for file_path in json_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        count = 0
                        # Handle wrapped data (from _write_json metadata wrapper)
                        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                            all_records.extend(data['data'])
                            count = len(data['data'])
                        elif isinstance(data, dict) and 'jobs' in data and isinstance(data['jobs'], list):
                            all_records.extend(data['jobs'])
                            count = len(data['jobs'])
                        elif isinstance(data, list):
                            all_records.extend(data)
                            count = len(data)
                        elif isinstance(data, dict):
                            all_records.append(data)
                            count = 1
                            
                        logger.info(f"Read {count} records from {file_path.name}")
                        
                except Exception as e:
                    logger.warning(f"Failed to read {file_path}: {e}")
            
            logger.info(f"Total records read from partition: {len(all_records)}")
            
            if not all_records:
                logger.warning(f"No records found in JSON files at {partition_path}")
                return pd.DataFrame()
                
            return pd.DataFrame(all_records)
            
        else:
            raise NotImplementedError(f"Reading all files not implemented for format: {self.format}")
    
    def read_latest(self) -> Any:
        """Read the most recent partition"""
        # Find most recent partition
        partitions = self._list_partitions()
        if not partitions:
            raise FileNotFoundError(f"No partitions found in {self.base_path}")
        
        latest = sorted(partitions)[-1]
        return self._read_partition_all(latest)
    
    def _list_partitions(self) -> List[Path]:
        """List all partition paths"""
        partitions = []
        
        # Recursively find leaf directories (deepest partition level)
        for path in self.base_path.rglob('*'):
            if path.is_dir():
                # Check if this is a leaf directory (contains files, not subdirs with partitions)
                has_data_files = any(
                    f.suffix in ['.json', '.parquet'] 
                    for f in path.iterdir() 
                    if f.is_file()
                )
                if has_data_files:
                    partitions.append(path)
        
        return partitions
    
    def read_all(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Read all partitions within a date range
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
        
        Returns:
            Concatenated DataFrame
        """
        partitions = self._list_partitions()
        
        # Filter by date range if provided
        if start_date or end_date:
            filtered_partitions = []
            for partition in partitions:
                partition_date = self._extract_partition_date(partition)
                if partition_date:
                    if start_date and partition_date < start_date:
                        continue
                    if end_date and partition_date > end_date:
                        continue
                    filtered_partitions.append(partition)
            partitions = filtered_partitions
        
        if not partitions:
            raise FileNotFoundError("No partitions found in date range")
        
        # Read and concatenate all partitions
        dfs = []
        for partition in partitions:
            try:
                df = self._read_partition_all(partition)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read partition {partition}: {e}")
        
        if not dfs:
            raise ValueError("No data found in partitions")
        
        return pd.concat(dfs, ignore_index=True)
    
    def _extract_partition_date(self, partition_path: Path) -> Optional[datetime]:
        """Extract date from partition path"""
        # Parse year/month/day from path
        parts = partition_path.parts
        year = month = day = None
        
        for part in parts:
            if part.startswith('year='):
                year = int(part.split('=')[1])
            elif part.startswith('month='):
                month = int(part.split('=')[1])
            elif part.startswith('day='):
                day = int(part.split('=')[1])
        
        if year and month:
            return datetime(year, month, day or 1)
        
        return None


def incremental_merge(
    new_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    key_column: str = 'job_id'
) -> pd.DataFrame:
    """
    Merge new data with existing data, deduplicating by key
    
    Args:
        new_df: New data to merge
        existing_df: Existing data
        key_column: Column to use for deduplication
    
    Returns:
        Merged DataFrame with duplicates removed
    """
    # Concatenate
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Deduplicate, keeping newest records
    combined = combined.drop_duplicates(subset=[key_column], keep='last')
    
    logger.info(
        f"Incremental merge: {len(existing_df)} existing + {len(new_df)} new "
        f"= {len(combined)} total records"
    )
    
    return combined
