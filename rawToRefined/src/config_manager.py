"""
config_manager.py - Configuration management utilities
"""

import yaml
import os
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manage all configuration files"""
    
    def __init__(self, config_dir: str):
        """
        Initialize configuration manager
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = config_dir
        self.app_config = {}
        self.s3_paths = {}
        self.sql_queries = {}
        
        self._load_all_configs()
    
    def _load_yaml(self, file_path: str) -> Dict:
        """Load YAML file"""
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def _load_all_configs(self):
        """Load all configuration files"""
        # Load app config
        app_config_path = os.path.join(self.config_dir, 'app_config.yaml')
        self.app_config = self._load_yaml(app_config_path)
        logger.info(f"Loaded app configuration from {app_config_path}")
        
        # Load S3 paths
        s3_paths_path = os.path.join(self.config_dir, 's3_paths.yaml')
        self.s3_paths = self._load_yaml(s3_paths_path)
        logger.info(f"Loaded S3 paths from {s3_paths_path}")
        
        # Load SQL queries
        sql_dir = os.path.join(self.config_dir, 'sql_queries')
        self._load_sql_queries(sql_dir)
    
    def _load_sql_queries(self, sql_dir: str):
        """Load all SQL query files"""
        sql_files = {
            'bronze_to_silver': os.path.join(sql_dir, 'bronze_to_silver.yaml'),
            'silver_to_gold': os.path.join(sql_dir, 'silver_to_gold.yaml')
        }
        
        for key, file_path in sql_files.items():
            if os.path.exists(file_path):
                self.sql_queries[key] = self._load_yaml(file_path)
                logger.info(f"Loaded SQL queries from {file_path}")
    
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