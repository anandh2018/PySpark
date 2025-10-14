"""
main.py - Main application entry point
"""

import os
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
    """Main report application"""
    
    def __init__(self, config_dir: str):
        """
        Initialize application
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = config_dir
        self.start_time = datetime.now()
        
        # Load configurations
        self.config = ConfigManager(config_dir)
        
        # Initialize Spark
        self.spark = self._init_spark()
        
        # Initialize components
        self.data_loader = DataLoader(self.spark, self.config)
        self.sql_executor = SQLExecutor(self.spark, self.config)
    
    def _init_spark(self) -> SparkSession:
        """Initialize Spark session"""
        spark_config = self.config.get_spark_config()
        app_name = self.config.app_config['application']['name']
        
        logger.info(f"Initializing Spark session: {app_name}")
        
        builder = SparkSession.builder.appName(app_name)
        
        # Apply configurations
        for key, value in spark_config.get('configs', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        
        return spark
    
    def run(self):
        """Execute the complete report workflow"""
        logger.info("\n" + "=" * 80)
        logger.info("CFA SF STRUCS PARENT REPORT - START")
        logger.info("=" * 80)
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Load Bronze Tables from S3
            self.data_loader.load_bronze_tables()
            
            # Step 2: Execute Bronze to Silver Transformations
            self.sql_executor.execute_bronze_to_silver()
            
            # Step 3: Execute Silver to Gold Transformations
            self.sql_executor.execute_silver_to_gold()
            
            # Calculate runtime
            end_time = datetime.now()
            runtime = (end_time - self.start_time).total_seconds() / 60
            
            logger.info("\n" + "=" * 80)
            logger.info("CFA SF STRUCS PARENT REPORT - COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total runtime: {runtime:.2f} minutes")
            
            return True
            
        except Exception as e:
            logger.error("\n" + "=" * 80)
            logger.error("CFA SF STRUCS PARENT REPORT - FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {str(e)}", exc_info=True)
            raise
        
        finally:
            # Stop Spark
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    # Get configuration directory
    config_dir = os.getenv('CONFIG_DIR', './config')
    
    # Run report
    report = CFASFStrucsParentReport(config_dir)
    success = report.run()
    
    return success


if __name__ == "__main__":
    main()
