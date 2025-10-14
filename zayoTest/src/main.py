"""
main.py - Main application entry point with S3 config support
"""

import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from config_manager import ConfigManager
from data_loader import DataLoader
from sql_executor import SQLExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CFASFStrucsParentReport:
    """Main report application with S3 config support"""
    
    def __init__(self, s3_config_bucket: str, s3_config_prefix: str):
        """
        Initialize application
        
        Args:
            s3_config_bucket: S3 bucket containing config files
            s3_config_prefix: S3 prefix for config files
        """
        self.s3_config_bucket = s3_config_bucket
        self.s3_config_prefix = s3_config_prefix
        self.start_time = datetime.now()
        
        logger.info("\n" + "=" * 80)
        logger.info("CFA SF STRUCS PARENT REPORT - INITIALIZATION")
        logger.info("=" * 80)
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Config location: s3://{s3_config_bucket}/{s3_config_prefix}")
        
        # Load configurations from S3
        self.config = ConfigManager(s3_config_bucket, s3_config_prefix)
        
        # Print configuration summary
        self._print_config_summary()
        
        # Initialize Spark
        self.spark = self._init_spark()
        
        # Initialize components
        self.data_loader = DataLoader(self.spark, self.config)
        self.sql_executor = SQLExecutor(self.spark, self.config)
    
    def _print_config_summary(self):
        """Print summary of loaded configurations"""
        summary = self.config.get_config_summary()
        
        logger.info("\nConfiguration Summary:")
        logger.info(f"  Bronze tables: {summary['bronze_tables_count']}")
        logger.info(f"  Silver tables: {summary['silver_tables_count']}")
        logger.info(f"  Gold reports: {summary['gold_reports_count']}")
        logger.info(f"  Bronze→Silver queries: {summary['bronze_to_silver_queries']}")
        logger.info(f"  Silver→Gold queries: {summary['silver_to_gold_queries']}")
    
    def _init_spark(self) -> SparkSession:
        """Initialize Spark session"""
        spark_config = self.config.get_spark_config()
        app_config = self.config.get_application_config()
        app_name = app_config.get('name', 'CFA_SF_STRUCS_PARENT_REPORT')
        
        logger.info(f"\nInitializing Spark session: {app_name}")
        
        builder = SparkSession.builder.appName(app_name)
        
        # Apply configurations
        for key, value in spark_config.get('configs', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        logger.info(f"  ✓ Spark version: {spark.version}")
        logger.info(f"  ✓ Spark UI: {spark.sparkContext.uiWebUrl}")
        
        return spark
    
    def run(self):
        """Execute the complete report workflow"""
        logger.info("\n" + "=" * 80)
        logger.info("CFA SF STRUCS PARENT REPORT - EXECUTION START")
        logger.info("=" * 80)
        
        try:
            # Step 1: Load Bronze Tables from S3
            bronze_tables = self.data_loader.load_bronze_tables()
            logger.info(f"\n✓ Step 1 Complete: Loaded {len(bronze_tables)} bronze tables")
            
            return True
            
        except Exception as e:
            end_time = datetime.now()
            runtime = (end_time - self.start_time).total_seconds() / 60
            
            logger.error("\n" + "=" * 80)
            logger.error("CFA SF STRUCS PARENT REPORT - FAILED ✗")
            logger.error("=" * 80)
            logger.error(f"Error: {str(e)}", exc_info=True)
            logger.error(f"Failed after: {runtime:.2f} minutes")
            logger.error("=" * 80)
            raise
        
        finally:
            # Stop Spark
            logger.info("\nStopping Spark session...")
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    
    # Get S3 config location from environment variables
    s3_config_bucket = "m6-sf-sync-s3-dev"
    s3_config_prefix = "rawToRefinedDataLoad/configs"
    
    if not s3_config_bucket:
        logger.error("ERROR: S3_CONFIG_BUCKET environment variable not set")
        logger.error("Please set: export S3_CONFIG_BUCKET=your-config-bucket")
        sys.exit(1)
    
    logger.info(f"Configuration source: s3://{s3_config_bucket}/{s3_config_prefix}")
    
    try:
        # Run report
        report = CFASFStrucsParentReport(s3_config_bucket, s3_config_prefix)
        success = report.run()
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
