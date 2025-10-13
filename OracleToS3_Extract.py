from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import yaml
from google.cloud import storage
import logging
import argparse
from typing import Dict, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OracleDataExtractor:
    def __init__(self, config_bucket: str, config_path: str):
        self.config_bucket = config_bucket
        self.config_path = config_path
        self.config = None
        self.spark = None
        
    def read_config_from_gcs(self):
        logger.info("Read configuration file from GCS bucket")
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.config_bucket)
            blob = bucket.blob(self.config_path)
            
            config_content = blob.download_as_string().decode('utf-8')
            
            # Read Yaml file which has source connection and extraction sql logic details
            if self.config_path.endswith(('.yaml', '.yml')):
                self.config = yaml.safe_load(config_content)
            else:
                raise ValueError("Config file must be yaml/yml formatted file")
            
            logger.info(f"Configuration loaded successfully from gs://{self.config_bucket}/{self.config_path}")
            return self.config
            
        except Exception as e:
            logger.error(f"Error reading config from GCS: {str(e)}")
            raise
    
    def create_spark_session(self):
        logger.info("Create Spark session")
        oracle_config = self.config.get('oracle', {})
        spark_config = self.config.get('spark', {})
        
        # Build Spark session with optimized settings
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'OracleDataExtraction'))
        
        # Apply custom Spark configurations for performance
        spark_configs = {
            # Memory settings
            'spark.executor.memory': spark_config.get('executor_memory', '8g'),
            'spark.driver.memory': spark_config.get('driver_memory', '4g'),
            'spark.memory.fraction': spark_config.get('memory_fraction', '0.8'),
            'spark.memory.storageFraction': spark_config.get('storage_fraction', '0.3'),
            
            # Parallelism settings
            'spark.sql.shuffle.partitions': spark_config.get('shuffle_partitions', '400'),
            'spark.default.parallelism': spark_config.get('default_parallelism', '400'),
            
            # Network settings
            'spark.network.timeout': spark_config.get('network_timeout', '600s'),
            'spark.executor.heartbeatInterval': spark_config.get('heartbeat_interval', '60s'),
            
            # JDBC settings for Oracle
            'spark.sql.sources.parallelPartitionDiscovery.parallelism': '32',
        }
        
        # Apply all configurations
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel(spark_config.get('log_level', 'WARN'))
        
        logger.info("Spark session created successfully")
        return self.spark
    def get_table_config(self, table_name: str) -> Optional[Dict]:

        tables_config = self.config.get('tables', {})
        
        if table_name not in tables_config:
            logger.error(f"Table '{table_name}' not found in configuration")
            available_tables = list(tables_config.keys())
            logger.error(f"Available tables: {', '.join(available_tables)}")
            return None

        return tables_config[table_name]
    
    def extract_data_parallel(self, table_config: Dict, output_path: str):
        oracle_config = self.config.get('oracle', {})
        
        # Build JDBC URL
        jdbc_url = (
            f"jdbc:oracle:thin:@{oracle_config['host']}:"
            f"{oracle_config.get('port', 1521)}:"
            f"{oracle_config['service_name']}"
        )
        
        # Connection properties
        connection_properties = {
            "user": oracle_config['username'],
            "password": oracle_config['password'],
            "driver": "oracle.jdbc.OracleDriver",
            "fetchsize": str(table_config.get('fetch_size', 10000)),
            "batchsize": str(table_config.get('batch_size', 10000)),
            "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
        }
        
        # Get extraction query
        query = table_config['query']
        partition_column = table_config.get('partition_column')
        num_partitions = table_config.get('num_partitions', 100)
        
        logger.info(f"Starting parallel data extraction...")
        logger.info(f"Partition Column: {partition_column}")
        logger.info(f"Number of Partitions: {num_partitions}")
        
        try:
            # Get partition bounds
            lower_bound = table_config.get('lower_bound')
            upper_bound = table_config.get('upper_bound')
            
            # If bounds not provided, fetch them
            if not lower_bound or not upper_bound:
                logger.info("Fetching partition bounds...")
                bounds_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM ({query}) t) bounds"
                bounds_df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=bounds_query,
                    properties=connection_properties
                )
                bounds = bounds_df.collect()[0]
                lower_bound = bounds['min_val']
                upper_bound = bounds['max_val']
                logger.info(f"Bounds: {lower_bound} to {upper_bound}")
            
            # Read with partitioning
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) spark_extract",
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=connection_properties
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error during parallel data extraction: {str(e)}")
            raise
    def extract_data_sequential(self, table_config: Dict, output_path: str):
        oracle_config = self.config.get('oracle', {})
        
        # Build JDBC URL
        jdbc_url = (
            f"jdbc:oracle:thin:@{oracle_config['host']}:"
            f"{oracle_config.get('port', 1521)}:"
            f"{oracle_config['service_name']}"
        )
        
        # Connection properties
        connection_properties = {
            "user": oracle_config['username'],
            "password": oracle_config['password'],
            "driver": "oracle.jdbc.OracleDriver",
            "fetchsize": str(table_config.get('fetch_size', 10000)),
            "batchsize": str(table_config.get('batch_size', 10000)),
            "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
        }
        
        query = table_config['query']
        
        logger.info(f"Starting sequential data extraction...")
        logger.warning("Sequential mode: This may be slower for large datasets")
        
        try:
            # Read without partitioning
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=f"({query}) spark_extract",
                properties=connection_properties
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error during sequential data extraction: {str(e)}")
            raise


    def write_data(self, df, output_path: str, table_config: Dict):
        """Write dataframe to output path with optimal settings"""
        output_format = table_config.get('output_format', 'parquet')
        output_partitions = table_config.get('output_partitions', 200)
        
        # Repartition for output
        df = df.repartition(output_partitions)
        
        logger.info(f"Writing data to {output_path}...")
        
        # Write options
        write_options = {
            'compression': table_config.get('compression', 'snappy'),
            'mode': table_config.get('write_mode', 'overwrite')
        }
        
        if output_format == 'parquet':
            df.write.parquet(output_path, **write_options)
        elif output_format == 'orc':
            df.write.orc(output_path, **write_options)
        elif output_format == 'csv':
            df.write.csv(output_path, header=True, **write_options)
        else:
            df.write.format(output_format).save(output_path, **write_options)
        
        logger.info(f"Data written successfully to {output_path}")
    def extract_table(self, table_name: str, output_base_path: str):
        # Get table configuration
        table_config = self.get_table_config(table_name)
        if not table_config:
            raise ValueError(f"Configuration for table '{table_name}' not found")
        
        # Determine output path
        output_path = f"{output_base_path.rstrip('/')}/{table_name}/"
        
        logger.info("="*80)
        logger.info(f"Extracting table: {table_name}")
        logger.info(f"Output path: {output_path}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Check if parallel execution is enabled
            parallel_execution = table_config.get('parallel_execution', True)
            
            if parallel_execution and table_config.get('partition_column'):
                logger.info("Using PARALLEL extraction mode")
                df = self.extract_data_parallel(table_config, output_path)
            else:
                if parallel_execution and not table_config.get('partition_column'):
                    logger.warning("Parallel execution requested but no partition_column specified")
                    logger.warning("Falling back to SEQUENTIAL mode")
                else:
                    logger.info("Using SEQUENTIAL extraction mode")
                df = self.extract_data_sequential(table_config, output_path)
            
            # Cache if specified for complex transformations
            if table_config.get('cache_dataframe', False):
                logger.info("Caching dataframe for optimization...")
                df.cache()
            
            # Write data
            self.write_data(df, output_path, table_config)
            
            # Get statistics
            record_count = df.count()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("="*80)
            logger.info(f"Table: {table_name} - Extraction Summary")
            logger.info(f"Records extracted: {record_count:,}")
            logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            logger.info(f"Throughput: {record_count/duration:,.0f} records/second")
            logger.info("="*80)
            
            return record_count
            
        except Exception as e:
            logger.error(f"Error extracting table '{table_name}': {str(e)}")
            raise
    
    def extract_all_tables(self, output_base_path: str):

        tables_config = self.config.get('tables', {})
        
        if not tables_config:
            logger.error("No tables defined in configuration")
            return
        
        total_start_time = datetime.now()
        results = {}
        
        logger.info(f"Starting extraction for {len(tables_config)} table(s)")
        
        for table_name in tables_config.keys():
            try:
                record_count = self.extract_table(table_name, output_base_path)
                results[table_name] = {'status': 'SUCCESS', 'records': record_count}
            except Exception as e:
                logger.error(f"Failed to extract table '{table_name}': {str(e)}")
                results[table_name] = {'status': 'FAILED', 'error': str(e)}
        
        total_end_time = datetime.now()
        total_duration = (total_end_time - total_start_time).total_seconds()
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
        
        success_count = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
        failed_count = len(results) - success_count
        total_records = sum(r.get('records', 0) for r in results.values() if r['status'] == 'SUCCESS')
        
        for table_name, result in results.items():
            if result['status'] == 'SUCCESS':
                logger.info(f"✓ {table_name}: {result['records']:,} records")
            else:
                logger.error(f"✗ {table_name}: FAILED - {result.get('error', 'Unknown error')}")
        
        logger.info("="*80)
        logger.info(f"Total tables processed: {len(results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Total records: {total_records:,}")
        logger.info(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        logger.info("="*80)
        
        return results
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution function"""
    
    # Configuration
    parser = argparse.ArgumentParser(description='Oracle to PySpark Data Extraction')
    parser.add_argument('--config-bucket', required=True, help='GCS bucket containing config file')
    parser.add_argument('--config-path', required=True, help='Path to config file in bucket')
    parser.add_argument('--output-path', required=True, help='Base output path in GCS')
    parser.add_argument('--table-name', help='Specific table to extract (optional, extracts all if not specified)')
    
    args = parser.parse_args()
    
    try:
        # Initialize extractor
        extractor = OracleDataExtractor(args.config_bucket, args.config_path)
        
        # Load configuration
        extractor.read_config_from_gcs()
        
        # Create Spark session
        extractor.create_spark_session()

        if args.table_name:
            # Extract specific table
            logger.info(f"Extracting specific table: {args.table_name}")
            record_count = extractor.extract_table(args.table_name, args.output_path)
            logger.info(f"Extraction completed! Total records: {record_count:,}")
        else:
            # Extract all tables
            logger.info("Extracting all tables from configuration")
            extractor.extract_all_tables(args.output_path)

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise
    finally:
        # Cleanup
        extractor.cleanup()


if __name__ == "__main__":
    main()
