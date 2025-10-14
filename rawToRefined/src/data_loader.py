"""
data_loader.py - Load data from S3 and register as Spark DataFrames
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from config_manager import ConfigManager

logger = logging.getLogger(__name__)


class DataLoader:
    """Load data from S3 parquet files and register as temp views"""
    
    def __init__(self, spark: SparkSession, config: ConfigManager):
        """
        Initialize Data Loader
        
        Args:
            spark: SparkSession object
            config: ConfigManager instance
        """
        self.spark = spark
        self.config = config
    
    def load_bronze_tables(self) -> Dict[str, DataFrame]:
        """
        Load all bronze layer tables from S3
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        logger.info("=" * 60)
        logger.info("STEP 1: LOADING BRONZE LAYER (Oracle Data from S3)")
        logger.info("=" * 60)
        
        bronze_tables = self.config.get_bronze_tables()
        loaded_tables = {}
        
        for table_name, table_config in bronze_tables.items():
            path = table_config['path']
            file_format = table_config.get('format', 'parquet')
            view_name = table_config.get('view_name', table_name)
            
            logger.info(f"Loading table: {table_name}")
            logger.info(f"  Path: {path}")
            logger.info(f"  Format: {file_format}")
            
            try:
                # Read from S3
                df = self.spark.read.format(file_format).load(path)
                
                # Register as temp view
                df.createOrReplaceTempView(view_name)
                loaded_tables[table_name] = df
                
                # Log statistics
                record_count = df.count()
                column_count = len(df.columns)
                logger.info(f"  ✓ Loaded: {record_count:,} records, {column_count} columns")
                logger.info(f"  ✓ Registered as view: {view_name}")
                
            except Exception as e:
                logger.error(f"  ✗ Error loading {table_name}: {str(e)}")
                raise
        
        logger.info(f"\n✓ Successfully loaded {len(loaded_tables)} bronze tables")
        return loaded_tables