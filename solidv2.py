import logging
import functools
import json
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Union, Any
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import StringSerializer
import requests


class DataStoreType(Enum):
    """Enumeration of supported data store types"""
    KAFKA = "kafka"
    ADLS = "adls"


class ApplicationConfig:
    """
    Central application configuration that manages environment-specific configurations
    """
    
    def __init__(self):
        self._config_dict = {
            "nonprod": {
                # Kafka Configuration
                "kafka": {
                    "bootstrap-servers": "nonprod-kafka-broker1:9092,nonprod-kafka-broker2:9092",
                    "client-id-key": "nonprod-kafka-client-id",
                    "client-secret-key": "nonprod-kafka-client-secret",
                    "token-url": "https://nonprod-auth.company.com/oauth/token",
                    "ssl-ca-location": "kafka-nonprod.pem",
                    "default-group-id": "nonprod-insurance-data-eng"
                },
                # ADLS Configuration
                "adls": {
                    "client-id-key": "nonprod-adls-client-id",
                    "client-secret-key": "nonprod-adls-client-secret",
                    "tenant-id-key": "nonprod-tenant-id"  # Changed to key reference
                },
                # Common Configuration
                "default-secret-scope": "nonprod-secrets"
            },
            "prod": {
                # Kafka Configuration
                "kafka": {
                    "bootstrap-servers": "prod-kafka-broker1:9092,prod-kafka-broker2:9092",
                    "client-id-key": "prod-kafka-client-id",
                    "client-secret-key": "prod-kafka-client-secret",
                    "token-url": "https://prod-auth.company.com/oauth/token",
                    "ssl-ca-location": "kafka-prod.pem",
                    "default-group-id": "prod-insurance-data-eng"
                },
                # ADLS Configuration
                "adls": {
                    "client-id-key": "prod-adls-client-id", 
                    "client-secret-key": "prod-adls-client-secret",
                    "tenant-id-key": "prod-tenant-id"  # Changed to key reference
                },
                # Common Configuration
                "default-secret-scope": "prod-secrets"
            }
        }
    
    def get_config_dict(self) -> Dict[str, Any]:
        """Get the complete configuration dictionary"""
        return self._config_dict.copy()
    
    def get_configuration(self, env: str, service_type: str) -> Dict[str, Any]:
        """
        Get configuration for a specific environment and service type
        
        Args:
            env (str): Environment name ('prod' or 'nonprod')
            service_type (str): Service type ('kafka', 'adls', etc.)
            
        Returns:
            dict: Configuration dictionary for the service
        """
        if env not in self._config_dict:
            raise ValueError(f"Environment '{env}' not found in configurations")
        
        env_config = self._config_dict[env]
        
        if service_type not in env_config:
            raise ValueError(f"Service type '{service_type}' not found for environment '{env}'")
        
        # Merge service-specific config with common config
        config = env_config[service_type].copy()
        config['default-secret-scope'] = env_config['default-secret-scope']
        
        return config
    
    def add_environment_config(self, env: str, service_type: str, config_dict: Dict[str, Any]):
        """
        Add or update configuration for an environment and service type
        
        Args:
            env (str): Environment name
            service_type (str): Service type
            config_dict (dict): Configuration dictionary
        """
        if env not in self._config_dict:
            self._config_dict[env] = {}
        
        self._config_dict[env][service_type] = config_dict


class ConfigurationService:
    """
    Configuration service that provides a unified interface for accessing configurations
    and resolving secrets from Databricks
    """
    
    def __init__(self, application_config: Optional[ApplicationConfig] = None):
        self.application_config = application_config or ApplicationConfig()
        self.logger = logging.getLogger(__name__)
    
    def get_configuration(self, env: str, service_type: str = None) -> Dict[str, Any]:
        """
        Get configuration for environment and optional service type
        
        Args:
            env (str): Environment name
            service_type (str): Optional service type filter
            
        Returns:
            dict: Configuration dictionary
        """
        if service_type:
            return self.application_config.get_configuration(env, service_type)
        else:
            # Return all configurations for the environment
            return self.application_config.get_config_dict()[env]
    
    def get_resolved_configuration(self, env: str, service_type: str, 
                                 custom_secret_scope: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration with secrets resolved from Databricks
        
        Args:
            env (str): Environment name
            service_type (str): Service type
            custom_secret_scope (str): Optional custom secret scope
            
        Returns:
            dict: Configuration with resolved secret values
        """
        config = self.get_configuration(env, service_type)
        secret_scope = custom_secret_scope or config.get('default-secret-scope')
        
        resolved_config = config.copy()
        
        # Resolve secrets for keys that end with '-key'
        for key, value in config.items():
            if key.endswith('-key'):
                try:
                    resolved_value = self._get_secret(secret_scope, value)
                    # Store resolved value with new key name (remove '-key' suffix)
                    resolved_key = key.replace('-key', '')
                    resolved_config[resolved_key] = resolved_value
                    self.logger.info(f"Resolved secret for {key}")
                except Exception as e:
                    self.logger.error(f"Failed to resolve secret for {key}: {str(e)}")
                    raise Exception(f"Failed to resolve secret for {key}: {str(e)}")
        
        return resolved_config
    
    def _get_secret(self, scope: str, key: str) -> str:
        """
        Get secret value from Databricks secret scope
        
        Args:
            scope (str): Secret scope name
            key (str): Secret key name
            
        Returns:
            str: Secret value
        """
        try:
            # In a real Databricks environment, this would be:
            # return dbutils.secrets.get(scope, key)
            
            # For demonstration/testing purposes:
            return f"resolved_secret_from_{scope}_{key}"
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve secret '{key}' from scope '{scope}': {str(e)}")
            raise


class DataStoreConnectionFactory:
    """
    Factory class for creating data store connections based on type
    """
    
    @staticmethod
    def create_connection(datastore_type: DataStoreType, env: str, 
                         config_service: Optional[ConfigurationService] = None,
                         **kwargs):
        """
        Create a connection based on the datastore type
        
        Args:
            datastore_type (DataStoreType): Type of datastore to connect to
            env (str): Environment name
            config_service (ConfigurationService): Configuration service instance
            **kwargs: Additional arguments for specific connectors
            
        Returns:
            Connection object based on the datastore type
        """
        config_service = config_service or ConfigurationService()
        
        if datastore_type == DataStoreType.ADLS:
            return ADLSConnector(env=env, config_service=config_service, **kwargs)
        elif datastore_type == DataStoreType.KAFKA:
            return KafkaProducer(env=env, config_service=config_service, **kwargs)
        else:
            raise ValueError(f"Unsupported datastore type: {datastore_type}")


class ADLSConnector:
    """
    Azure Data Lake Storage connector implementation
    """
    
    def __init__(self, env: str = 'nonprod', config_service: Optional[ConfigurationService] = None, 
                 safe_parent_path: str = "/mnt/temp", custom_secret_scope: Optional[str] = None):
        """
        Initialize the ADLS connector
        
        Args:
            env (str): Environment ('prod' or 'nonprod')
            config_service (ConfigurationService): Configuration service instance
            safe_parent_path (str): Safe parent path for operations
            custom_secret_scope (str): Optional custom secret scope
        """
        self.env = env
        self.config_service = config_service or ConfigurationService()
        self.safe_parent_path = safe_parent_path
        self.custom_secret_scope = custom_secret_scope
        self.logger = logging.getLogger(__name__)
        
        # Get resolved configuration (secrets resolved by ConfigurationService)
        self.env_config = self.config_service.get_resolved_configuration(
            env, 'adls', custom_secret_scope
        )
        
        self.config_dict = None
        self.source_path = None
        self.mount_point = None
        self.is_mounted = False
        
    def setup_connection(self, storage_account_name: str, container_name: str, 
                        mount_name: str, subfolder_path: str = ""):
        """
        Set up the ADLS connection configuration
        
        Args:
            storage_account_name (str): Azure storage account name
            container_name (str): Container name
            mount_name (str): Local mount name
            subfolder_path (str): Optional subfolder path
        """
        # Use resolved secrets from configuration service
        client_id = self.env_config['client-id']
        client_secret = self.env_config['client-secret']
        tenant_id = self.env_config['tenant-id']
        
        # Build Azure storage path
        self.source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{subfolder_path}"
        self.mount_point = f"/mnt/{mount_name}"
        
        # Create OAuth configuration
        self.config_dict = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        }
        
        self.logger.info(f"ADLS configuration set up for {self.env} environment")
        self.logger.info(f"Source: {self.source_path}, Mount: {self.mount_point}")
    
    def mount(self):
        """Mount the ADLS storage to Databricks file system"""
        if not self.config_dict:
            raise Exception("Configuration not set up. Call setup_connection() first.")
        
        try:
            # In real Databricks environment:
            # dbutils.fs.mount(
            #     source=self.source_path,
            #     mount_point=self.mount_point,
            #     extra_configs=self.config_dict
            # )
            self.is_mounted = True
            self.logger.info(f"Successfully mounted {self.source_path} at {self.mount_point}")
            
        except Exception as e:
            if "already mounted" in str(e).lower():
                self.is_mounted = True
                self.logger.info(f"{self.mount_point} already mounted")
            else:
                self.logger.error(f"Failed to mount ADLS: {str(e)}")
                raise Exception(f"Failed to mount ADLS: {str(e)}")
    
    def unmount(self):
        """Unmount the ADLS storage"""
        if not self.mount_point:
            raise Exception("No mount point configured")
            
        try:
            # In real Databricks environment:
            # dbutils.fs.unmount(self.mount_point)
            self.is_mounted = False
            self.logger.info(f"Successfully unmounted {self.mount_point}")
            
        except Exception as e:
            self.logger.error(f"Could not unmount {self.mount_point}: {str(e)}")
            raise


class KafkaProducer:
    """
    Kafka producer for publishing messages to topics
    """
    
    def __init__(self, env: str = 'nonprod', topic_name: Optional[str] = None, 
                 config_service: Optional[ConfigurationService] = None, 
                 custom_secret_scope: Optional[str] = None):
        """
        Initialize the Kafka producer
        
        Args:
            env (str): Environment ('prod' or 'nonprod')
            topic_name (str): Default topic name
            config_service (ConfigurationService): Configuration service instance
            custom_secret_scope (str): Custom secret scope
        """
        self.env = env
        self.topic_name = topic_name
        self.config_service = config_service or ConfigurationService()
        self.custom_secret_scope = custom_secret_scope
        self.logger = logging.getLogger(__name__)
        
        # Get resolved configuration (secrets resolved by ConfigurationService)
        self.env_config = self.config_service.get_resolved_configuration(
            env, 'kafka', custom_secret_scope
        )
        
        # Setup credentials from resolved config
        self.client_id = self.env_config['client-id']
        self.client_secret = self.env_config['client-secret']
        self.bootstrap_servers = self.env_config['bootstrap-servers']
        self.token_url = self.env_config['token-url']
        self.ssl_ca_location = self.env_config['ssl-ca-location']
        
        # Create producer
        self.producer = Producer(self._get_producer_config())
        self.serializer = StringSerializer('utf8')
        
        self.logger.info(f"Kafka Producer initialized for {self.env} environment")
        self.logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
    
    def _get_token(self, config):
        """Get OAuth token for authentication"""
        payload = {
            'grant_type': 'client_credentials',
            'scope': 'kafka'
        }
        
        resp = requests.post(
            self.token_url,
            auth=(self.client_id, self.client_secret),
            data=payload
        )
        
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])
    
    def _get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': 'sasl_ssl',
            'sasl.mechanisms': 'OAUTHBEARER',
            'ssl.ca.location': self.ssl_ca_location,
            'oauth_cb': functools.partial(self._get_token),
            'logger': self.logger,
        }
    
    def send_message(self, message: Union[Dict, str], topic_name: Optional[str] = None) -> bool:
        """
        Send a message to the specified topic
        
        Args:
            message (dict or str): Message to send
            topic_name (str): Topic to send to
            
        Returns:
            bool: Success status
        """
        if not topic_name and not self.topic_name:
            raise Exception("No topic specified. Provide topic_name parameter or set default topic.")
            
        target_topic = topic_name or self.topic_name
        
        # Convert message to JSON string if it's a dict
        if isinstance(message, dict):
            message = json.dumps(message)
        
        try:
            self.producer.produce(target_topic, value=message)
            self.producer.flush()
            self.logger.info(f"Message sent to topic '{target_topic}'")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False
    
    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        self.logger.info("Producer closed")


# Usage Examples:
if __name__ == "__main__":
    # Example 1: Using the factory pattern
    config_service = ConfigurationService()
    
    # Create ADLS connection using factory
    adls_conn = DataStoreConnectionFactory.create_connection(
        DataStoreType.ADLS, 
        env='nonprod',
        config_service=config_service
    )
    
    # Create Kafka connection using factory
    kafka_conn = DataStoreConnectionFactory.create_connection(
        DataStoreType.KAFKA,
        env='nonprod', 
        config_service=config_service,
        topic_name='my-topic'
    )
    
    # Example 2: Direct instantiation (still works)
    adls_direct = ADLSConnector(env='prod', config_service=config_service)