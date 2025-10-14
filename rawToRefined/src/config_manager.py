"""
config_manager.py - Configuration management with S3 support
"""

import yaml
import boto3
import logging
from typing import Dict, Any, List
from io import StringIO
import os

logger = logging.getLogger(__name__)


class S3ConfigLoader:
    """Load configuration files from S3"""
    
    def __init__(self, s3_config_bucket: str, s3_config_prefix: str):
        """
        Initialize S3 Config Loader
        
        Args:
            s3_config_bucket: S3 bucket containing config files
            s3_config_prefix: S3 prefix/folder for config files
        """
        self.s3_config_bucket = s3_config_bucket
        self.s3_config_prefix = s3_config_prefix.rstrip('/')
        self.s3_client = boto3.client('s3')
        
        logger.info(f"Initialized S3 Config Loader:")
        logger.info(f"  Bucket: {s3_config_bucket}")
        logger.info(f"  Prefix: {s3_config_prefix}")
    
    def load_yaml_from_s3(self, file_key: str) -> Dict:
        """
        Load YAML file from S3
        
        Args:
            file_key: S3 key (path) to the YAML file
            
        Returns:
            Dictionary with parsed YAML content
        """
        full_key = f"{self.s3_config_prefix}/{file_key}" if self.s3_config_prefix else file_key
        
        try:
            logger.info(f"Loading config from s3://{self.s3_config_bucket}/{full_key}")
            
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.s3_config_bucket,
                Key=full_key
            )
            
            # Read and parse YAML
            content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(StringIO(content))
            
            logger.info(f"  ✓ Successfully loaded config from S3")
            return config
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"  ✗ Config file not found in S3: {full_key}")
            raise FileNotFoundError(f"Config file not found: s3://{self.s3_config_bucket}/{full_key}")
        except Exception as e:
            logger.error(f"  ✗ Error loading config from S3: {str(e)}")
            raise
    
    def list_config_files(self, subfolder: str = "") -> List[str]:
        """
        List all config files in a specific folder
        
        Args:
            subfolder: Subfolder within the config prefix
            
        Returns:
            List of file keys
        """
        prefix = f"{self.s3_config_prefix}/{subfolder}".strip('/')
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_config_bucket,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
            
            logger.info(f"Found {len(files)} config files in s3://{self.s3_config_bucket}/{prefix}")
            return files
            
        except Exception as e:
            logger.error(f"Error listing config files: {str(e)}")
            raise
    
    def download_config_to_local(self, file_key: str, local_path: str):
        """
        Download config file from S3 to local filesystem
        
        Args:
            file_key: S3 key to download
            local_path: Local path to save the file
        """
        full_key = f"{self.s3_config_prefix}/{file_key}" if self.s3_config_prefix else file_key
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            logger.info(f"Downloading s3://{self.s3_config_bucket}/{full_key} to {local_path}")
            
            self.s3_client.download_file(
                self.s3_config_bucket,
                full_key,
                local_path
            )
            
            logger.info(f"  ✓ Downloaded successfully")
            
        except Exception as e:
            logger.error(f"  ✗ Error downloading file: {str(e)}")
            raise


class ConfigManager:
    """Manage all configuration files from S3"""
    
    def __init__(self, s3_config_bucket: str, s3_config_prefix: str):
        """
        Initialize configuration manager
        
        Args:
            s3_config_bucket: S3 bucket containing config files
            s3_config_prefix: S3 prefix for config files
        """
        self.s3_loader = S3ConfigLoader(s3_config_bucket, s3_config_prefix)
        self.app_config = {}
        self.s3_paths = {}
        self.sql_queries = {}
        
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all configuration files from S3"""
        logger.info("\n" + "=" * 60)
        logger.info("LOADING CONFIGURATIONS FROM S3")
        logger.info("=" * 60)
        
        try:
            # Load app config
            logger.info("\n1. Loading application configuration...")
            self.app_config = self.s3_loader.load_yaml_from_s3('app_config.yaml')
            
            # Load S3 paths
            logger.info("\n2. Loading S3 paths configuration...")
            self.s3_paths = self.s3_loader.load_yaml_from_s3('s3_paths.yaml')
            
            # Load SQL queries
            logger.info("\n3. Loading SQL query configurations...")
            self._load_sql_queries()
            
            logger.info("\n" + "=" * 60)
            logger.info("✓ ALL CONFIGURATIONS LOADED SUCCESSFULLY")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"\n✗ Failed to load configurations: {str(e)}")
            raise
    
    def _load_sql_queries(self):
        """Load all SQL query files from S3"""
        sql_configs = {
            'bronze_to_silver': 'sql_queries/bronze_to_silver.yaml',
            'silver_to_gold': 'sql_queries/silver_to_gold.yaml'
        }
        
        for key, file_key in sql_configs.items():
            try:
                self.sql_queries[key] = self.s3_loader.load_yaml_from_s3(file_key)
                queries_count = len(self.sql_queries[key].get('queries', {}))
                logger.info(f"  ✓ Loaded {queries_count} queries from {file_key}")
            except Exception as e:
                logger.warning(f"  ⚠ Could not load {file_key}: {str(e)}")
    
    def get_bronze_tables(self) -> Dict:
        """Get bronze layer table configurations"""
        return self.s3_paths.get('s3', {}).get('bronze', {}).get('tables', {})
    
    def get_silver_tables(self) -> Dict:
        """Get silver layer table configurations"""
        return self.s3_paths.get('s3', {}).get('silver', {}).get('tables', {})
    
    def get_gold_reports(self) -> Dict:
        """Get gold layer report configurations"""
        return self.s3_paths.get('s3', {}).get('gold', {}).get('reports', {})
    
    def get_bronze_to_silver_queries(self) -> Dict:
        """Get bronze to silver transformation queries"""
        return self.sql_queries.get('bronze_to_silver', {}).get('queries', {})
    
    def get_silver_to_gold_queries(self) -> Dict:
        """Get silver to gold transformation queries"""
        return self.sql_queries.get('silver_to_gold', {}).get('queries', {})
    
    def get_execution_order(self, layer: str) -> List[str]:
        """
        Get execution order for a specific layer
        
        Args:
            layer: 'bronze_to_silver' or 'silver_to_gold'
        """
        return self.sql_queries.get(layer, {}).get('metadata', {}).get('execution_order', [])
    
    def get_spark_config(self) -> Dict:
        """Get Spark configuration"""
        return self.app_config.get('spark', {})
    
    def get_processing_config(self) -> Dict:
        """Get processing configuration"""
        return self.app_config.get('processing', {})
    
    def get_application_config(self) -> Dict:
        """Get application configuration"""
        return self.app_config.get('application', {})
    
    def reload_configs(self):
        """Reload all configurations from S3"""
        logger.info("Reloading configurations from S3...")
        self._load_all_configs()
    
    def get_config_summary(self) -> Dict:
        """Get summary of loaded configurations"""
        return {
            'app_config_loaded': bool(self.app_config),
            's3_paths_loaded': bool(self.s3_paths),
            'bronze_tables_count': len(self.get_bronze_tables()),
            'silver_tables_count': len(self.get_silver_tables()),
            'gold_reports_count': len(self.get_gold_reports()),
            'bronze_to_silver_queries': len(self.get_bronze_to_silver_queries()),
            'silver_to_gold_queries': len(self.get_silver_to_gold_queries())
        }
