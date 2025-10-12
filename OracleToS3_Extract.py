from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import yaml
from google.cloud import storage
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OracleDataExtractor:
    def __init__(self, config_bucket: str, config_path: str):
        """
        Initialize the extractor with GCS config location
        
        Args:
            config_bucket: GCS bucket name
            config_path: Path to config file in bucket (supports .json or .yaml)
        """
        self.config_bucket = config_bucket
        self.config_path = config_path
        self.config = None
        self.spark = None
        
    def read_config_from_gcs(self):
        """Read configuration file from GCS bucket"""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.config_bucket)
            blob = bucket.blob(self.config_path)
            
            config_content = blob.download_as_string().decode('utf-8')
            
            # Read Yaml file which has source connection and extraction sql logic details
            self.config = yaml.safe_load(config_content)
             
            logger.info(f"Configuration loaded successfully from gs://{self.config_bucket}/{self.config_path}")
            return self.config
            
        except Exception as e:
            logger.error(f"Error reading config from GCS: {str(e)}")
            raise
    
    def create_spark_session(self):
        """Create optimized Spark session for large Oracle extraction"""
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
    
    def extract_data_parallel(self, output_path: str, output_format: str = 'parquet'):
        """
        Extract data from Oracle using parallel partitioning for optimal performance
        
        Args:
            output_path: GCS path to save extracted data (e.g., gs://bucket/path/)
            output_format: Output format (parquet, orc, csv, etc.)
        """
        oracle_config = self.config.get('oracle', {})
        extraction_config = self.config.get('extraction', {})
        
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
            "fetchsize": str(extraction_config.get('fetch_size', 10000)),
            "batchsize": str(extraction_config.get('batch_size', 10000)),
            "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'"
        }
        
        # Get extraction query
        query = extraction_config['query']
        
        # Partitioning strategy for 200M records
        partition_column = extraction_config.get('partition_column')
        num_partitions = extraction_config.get('num_partitions', 100)
        
        logger.info(f"Starting data extraction from Oracle...")
        logger.info(f"JDBC URL: {jdbc_url}")
        logger.info(f"Partition Column: {partition_column}")
        logger.info(f"Number of Partitions: {num_partitions}")
        
        try:
            if partition_column:
                # Parallel extraction with partitioning
                lower_bound = extraction_config.get('lower_bound')
                upper_bound = extraction_config.get('upper_bound')
                
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
                   # column=partition_column,
                   # lowerBound=lower_bound,
                   # upperBound=upper_bound,
                    numPartitions=num_partitions,
                    properties=connection_properties
                )
            else:
                # Non-partitioned read (not recommended for 200M records)
                logger.warning("No partition column specified. This may be slow for large datasets.")
                df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=f"({query}) spark_extract",
                    properties=connection_properties
                )
            
            # Apply any transformations if specified
            transformations = extraction_config.get('transformations', [])
            for transform in transformations:
                if transform['type'] == 'rename_column':
                    df = df.withColumnRenamed(transform['old_name'], transform['new_name'])
                elif transform['type'] == 'cast':
                    df = df.withColumn(transform['column'], col(transform['column']).cast(transform['data_type']))
            
            # Repartition if needed for output
            output_partitions = extraction_config.get('output_partitions', 200)
            df = df.repartition(output_partitions)
            
            logger.info(f"Data extraction complete. Writing to {output_path}...")
            
            # Write data with optimal settings
            write_options = {
                'compression': extraction_config.get('compression', 'snappy'),
                'mode': extraction_config.get('write_mode', 'overwrite')
            }

            df.write.parquet(output_path, **write_options)
            
            
            # Get statistics
            record_count = df.count()
            logger.info(f"Successfully extracted {record_count:,} records to {output_path}")
            
            return record_count
            
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution function"""
    
    # Configuration
    CONFIG_BUCKET = "liftAndShift"
    CONFIG_PATH = "pysparkJobConfig/oracle_extraction_config.yml"
    OUTPUT_PATH = "gs://liftAndShift/oracle_data/"
    OUTPUT_FORMAT = "parquet"
    
    try:
        # Initialize extractor
        extractor = OracleDataExtractor(CONFIG_BUCKET, CONFIG_PATH)
        
        # Load configuration
        extractor.read_config_from_gcs()
        
        # Create Spark session
        extractor.create_spark_session()
        
        # Extract data
        record_count = extractor.extract_data_parallel(OUTPUT_PATH, OUTPUT_FORMAT)
        
        logger.info(f"Extraction completed successfully! Total records: {record_count:,}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise
    finally:
        # Cleanup
        extractor.cleanup()


if __name__ == "__main__":
    main()
