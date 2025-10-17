"""
sql_executor.py - Execute SQL queries and save to staging layer
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List
from config_manager import ConfigManager
from datetime import datetime

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Execute SQL queries and manage data transformations"""
    
    def __init__(self, spark: SparkSession, config: ConfigManager):
        """
        Initialize SQL Executor
        
        Args:
            spark: SparkSession object
            config: ConfigManager instance
        """
        self.spark = spark
        self.config = config
        self.processing_config = config.get_processing_config()
    
    def execute_query(self, query_name: str, query_config: Dict) -> DataFrame:
        """
        Execute a single SQL query
        
        Args:
            query_name: Name of the query
            query_config: Query configuration dictionary
            
        Returns:
            DataFrame with query results
        """
        sql = query_config['sql']
        description = query_config.get('description', '')
        output_table = query_config.get('output_table', query_name)
        
        logger.info(f"\nExecuting: {query_name}")
        logger.info(f"Description: {description}")
        
        try:
            # Execute SQL
            start_time = datetime.now()
            df = self.spark.sql(sql)
            
            # Register as temp view
            df.createOrReplaceTempView(output_table)
            
            # Get statistics
            record_count = df.count()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"  ✓ Records: {record_count:,}")
            logger.info(f"  ✓ Execution time: {execution_time:.2f}s")
            logger.info(f"  ✓ Registered as view: {output_table}")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Error executing {query_name}: {str(e)}")
            raise
    
    def save_to_silver(self, df: DataFrame, table_name: str):
        """
        Save DataFrame to silver layer in S3
        
        Args:
            df: DataFrame to save
            table_name: Name of the table
        """
        silver_tables = self.config.get_silver_tables()
        
        if table_name not in silver_tables:
            logger.warning(f"Table {table_name} not found in silver configuration, skipping save")
            return
        
        table_config = silver_tables[table_name]
        path = table_config['path']
        partition_cols = table_config.get('partition_cols', [])
        
        output_config = self.processing_config.get('output', {})
        write_mode = output_config.get('write_mode', 'overwrite')
        file_format = output_config.get('format', 'parquet')
        compression = output_config.get('compression', 'snappy')
        coalesce = output_config.get('coalesce_partitions', 4)
        
        logger.info(f"  Saving to silver layer: {path}")
        
        try:
            writer = df.coalesce(coalesce).write.mode(write_mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            if file_format == 'parquet':
                writer.option('compression', compression).parquet(path)
            elif file_format == 'delta':
                writer.format('delta').save(path)
            else:
                writer.format(file_format).save(path)
            
            logger.info(f"  ✓ Saved to: {path}")
            
        except Exception as e:
            logger.error(f"  ✗ Error saving {table_name}: {str(e)}")
            raise
    
    def save_to_gold(self, df: DataFrame, report_name: str):
        """
        Save DataFrame to gold layer in S3
        
        Args:
            df: DataFrame to save
            report_name: Name of the report
        """
        gold_reports = self.config.get_gold_reports()
        
        if report_name not in gold_reports:
            logger.warning(f"Report {report_name} not found in gold configuration, skipping save")
            return
        
        report_config = gold_reports[report_name]
        path = report_config['path']
        partition_cols = report_config.get('partition_cols', [])
        
        output_config = self.processing_config.get('output', {})
        write_mode = output_config.get('write_mode', 'overwrite')
        file_format = output_config.get('format', 'parquet')
        compression = output_config.get('compression', 'snappy')
        coalesce = output_config.get('coalesce_partitions', 4)
        
        logger.info(f"  Saving to gold layer: {path}")
        
        try:
            writer = df.coalesce(coalesce).write.mode(write_mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            if file_format == 'parquet':
                writer.option('compression', compression).parquet(path)
            elif file_format == 'delta':
                writer.format('delta').save(path)
            else:
                writer.format(file_format).save(path)
            
            logger.info(f"  ✓ Saved to: {path}")
            
        except Exception as e:
            logger.error(f"  ✗ Error saving {report_name}: {str(e)}")
            raise
    
    def execute_bronze_to_silver(self):
        """Execute all bronze to silver transformations"""
        logger.info("=" * 60)
        logger.info("STEP 2: BRONZE TO SILVER TRANSFORMATIONS")
        logger.info("=" * 60)
        
        queries = self.config.get_bronze_to_silver_queries()
        execution_order = self.config.get_execution_order('bronze_to_silver')
        
        for query_name in execution_order:
            if query_name not in queries:
                logger.warning(f"Query {query_name} not found in configuration")
                continue
            
            query_config = queries[query_name]
            
            # Execute query
            df = self.execute_query(query_name, query_config)
            
            # Save to silver layer
            output_table = query_config.get('output_table', query_name)
            self.save_to_silver(df, output_table)
    
    def execute_silver_to_gold(self):
        """Execute all silver to gold transformations"""
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: SILVER TO GOLD TRANSFORMATIONS")
        logger.info("=" * 60)
        
        queries = self.config.get_silver_to_gold_queries()
        execution_order = self.config.get_execution_order('silver_to_gold')
        
        for query_name in execution_order:
            if query_name not in queries:
                logger.warning(f"Query {query_name} not found in configuration")
                continue
            
            query_config = queries[query_name]
            
            # Execute query
            df = self.execute_query(query_name, query_config)
            
            # Save to gold layer
            output_table = query_config.get('output_table', query_name)
            self.save_to_gold(df, output_table)