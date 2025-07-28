import logging
import functools
import json
import time
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import StringSerializer
import requests

class ApplicationConfigurationService:
    """
    Central configuration service that manages entire application configuration
    across multiple environments with topic definitions and secret resolution
    """
    
    def __init__(self):
        self._application_config = {
            "environments": {
                "nonprod": {
                    "kafka": {
                        "bootstrap-servers": "nonprod-kafka-broker1:9092,nonprod-kafka-broker2:9092",
                        "token-url": "https://nonprod-auth.company.com/oauth/token",
                        "ssl-ca-location": "kafka-nonprod.pem",
                        "default-group-id": "nonprod-insurance-data-eng",
                        "secret-scope": "nonprod-kafka-secrets",
                        "credentials": {
                            "client-id-secret-key": "kafka-client-id",
                            "client-secret-secret-key": "kafka-client-secret"
                        }
                    },
                    "adls": {
                        "tenant-id": "nonprod-tenant-id-value",
                        "secret-scope": "nonprod-adls-secrets",
                        "credentials": {
                            "client-id-secret-key": "adls-client-id",
                            "client-secret-secret-key": "adls-client-secret"
                        }
                    }
                },
                "prod": {
                    "kafka": {
                        "bootstrap-servers": "prod-kafka-broker1:9092,prod-kafka-broker2:9092",
                        "token-url": "https://prod-auth.company.com/oauth/token",
                        "ssl-ca-location": "kafka-prod.pem",
                        "default-group-id": "prod-insurance-data-eng",
                        "secret-scope": "prod-kafka-secrets",
                        "credentials": {
                            "client-id-secret-key": "kafka-client-id",
                            "client-secret-secret-key": "kafka-client-secret"
                        }
                    },
                    "adls": {
                        "tenant-id": "prod-tenant-id-value",
                        "secret-scope": "prod-adls-secrets",
                        "credentials": {
                            "client-id-secret-key": "adls-client-id",
                            "client-secret-secret-key": "adls-client-secret"
                        }
                    }
                },
                "staging": {
                    "kafka": {
                        "bootstrap-servers": "staging-kafka-broker1:9092",
                        "token-url": "https://staging-auth.company.com/oauth/token",
                        "ssl-ca-location": "kafka-staging.pem",
                        "default-group-id": "staging-insurance-data-eng",
                        "secret-scope": "staging-kafka-secrets",
                        "credentials": {
                            "client-id-secret-key": "kafka-client-id",
                            "client-secret-secret-key": "kafka-client-secret"
                        }
                    }
                }
            },
            "topics": {
                "insurance-claims": {
                    "purpose": "Processing new insurance claims submissions",
                    "data-format": "json",
                    "retention-days": 30,
                    "partitions": 6
                },
                "policy-updates": {
                    "purpose": "Policy modification and renewal events",
                    "data-format": "json",
                    "retention-days": 90,
                    "partitions": 3
                }
            },
            "consumer-groups": {
                "claims-processing-service": {
                    "purpose": "Main claims processing application",
                    "topics": ["insurance-claims", "fraud-alerts"]
                },
                "policy-management-service": {
                    "purpose": "Policy lifecycle management",
                    "topics": ["policy-updates", "customer-events"]
               }
            }
        }
    
    def get_configuration(self, environment, service_type=None):
        """
        Get configuration for a specific environment and optional service type
        
        Args:
            environment (str): Environment name ('prod', 'nonprod', 'staging')
            service_type (str): Optional service type ('kafka', 'adls', etc.)
            
        Returns:
            dict: Configuration dictionary
        """
        if environment not in self._application_config["environments"]:
            raise ValueError(f"Environment '{environment}' not found in configurations")
        
        env_config = self._application_config["environments"][environment]
        
        if service_type:
            if service_type not in env_config:
                raise ValueError(f"Service type '{service_type}' not found in environment '{environment}'")
            return env_config[service_type].copy()
        
        return env_config.copy()
    
    def get_topic_configuration(self, topic_name):
        """
        Get configuration for a specific topic
        
        Args:
            topic_name (str): Name of the topic
            
        Returns:
            dict: Topic configuration
        """
        if topic_name not in self._application_config["topics"]:
            raise ValueError(f"Topic '{topic_name}' not found in topic configurations")
        
        return self._application_config["topics"][topic_name].copy()
    
    def get_consumer_group_configuration(self, group_name):
        """
        Get configuration for a specific consumer group
        
        Args:
            group_name (str): Name of the consumer group
            
        Returns:
            dict: Consumer group configuration
        """
        if group_name not in self._application_config["consumer-groups"]:
            raise ValueError(f"Consumer group '{group_name}' not found in configurations")
        
        return self._application_config["consumer-groups"][group_name].copy()
    
    def list_topics(self):
        """Return list of all configured topics"""
        return list(self._application_config["topics"].keys())
    
    def list_environments(self):
        """Return list of all configured environments"""
        return list(self._application_config["environments"].keys())
    
    def list_consumer_groups(self):
        """Return list of all configured consumer groups"""
        return list(self._application_config["consumer-groups"].keys())
    
    def resolve_secrets(self, environment, service_type, credentials_config):
        """
        Resolve secret values from Databricks key vault
        
        Args:
            environment (str): Environment name
            service_type (str): Service type (kafka, adls, etc.)
            credentials_config (dict): Credentials configuration with secret keys
            
        Returns:
            dict: Resolved credentials with actual secret values
        """
        env_config = self.get_configuration(environment, service_type)
        secret_scope = env_config.get("secret-scope")
        
        if not secret_scope:
            raise Exception(f"No secret scope configured for {service_type} in {environment}")
        
        resolved_credentials = {}
        
        try:
            for credential_name, secret_key in credentials_config.items():
                # Remove '-secret-key' suffix to get the credential name
                clean_name = credential_name.replace('-secret-key', '')
                resolved_credentials[clean_name] = dbutils.secrets.get(secret_scope, secret_key)
                
        except Exception as e:
            raise Exception(f"Failed to resolve secrets from scope '{secret_scope}': {str(e)}")
        
        return resolved_credentials
    
    def add_environment(self, environment, config_dict):
        """Add a new environment configuration"""
        self._application_config["environments"][environment] = config_dict
    
    def add_topic(self, topic_name, topic_config):
        """Add a new topic configuration"""
        self._application_config["topics"][topic_name] = topic_config
    
    def add_consumer_group(self, group_name, group_config):
        """Add a new consumer group configuration"""
        self._application_config["consumer-groups"][group_name] = group_config


class KafkaProducer:
    """
    A class to produce (write) messages to Kafka topics.
    Designed for insurance data engineering workflows.
    """
    
    def __init__(self, environment='nonprod', topic_name=None, config_service=None):
        """
        Initialize the Kafka producer.
        
        Args:
            environment (str): Environment name ('prod', 'nonprod', 'staging')
            topic_name (str): Default topic to write to
            config_service (ApplicationConfigurationService): Optional config service instance
        """
        self.environment = environment
        self.config_service = config_service or ApplicationConfigurationService()
        self.topic_name = topic_name
        self.logger = logging.getLogger(__name__)
        
        # Get Kafka configuration for the environment
        self.kafka_config = self.config_service.get_configuration(environment, 'kafka')
        
        # Resolve credentials from Databricks secrets
        self.credentials = self.config_service.resolve_secrets(
            environment, 
            'kafka', 
            self.kafka_config['credentials']
        )
        
        # Validate topic if provided
        if self.topic_name:
            try:
                self.topic_config = self.config_service.get_topic_configuration(self.topic_name)
                print(f"Topic '{self.topic_name}' purpose: {self.topic_config['purpose']}")
            except ValueError as e:
                print(f"Warning: {e}")
                self.topic_config = None
        
        # Create producer with configuration
        self.producer = Producer(self._get_producer_config())
        self.serializer = StringSerializer('utf8')
        
        print(f"Kafka Producer initialized for {self.environment} environment")
        print(f"  Bootstrap servers: {self.kafka_config['bootstrap-servers']}")
        print(f"  Secret scope: {self.kafka_config['secret-scope']}")
        if self.topic_name:
            print(f"  Default topic: {self.topic_name}")
    
    def _get_token(self, config):
        """Get OAuth token for authentication."""
        payload = {
            'grant_type': 'client_credentials',
            'scope': 'kafka'
        }
        
        resp = requests.post(
            self.kafka_config['token-url'],
            auth=(self.credentials['client-id'], self.credentials['client-secret']),
            data=payload
        )
        
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])
    
    def _get_producer_config(self):
        """Get producer configuration."""
        return {
            'bootstrap.servers': self.kafka_config['bootstrap-servers'],
            'security.protocol': 'sasl_ssl',
            'sasl.mechanisms': 'OAUTHBEARER',
            'ssl.ca.location': self.kafka_config['ssl-ca-location'],
            'oauth_cb': functools.partial(self._get_token),
            'logger': self.logger,
        }
    
    def send_message(self, message, topic_name=None):
        """
        Send a message to the specified topic.
        
        Args:
            message (dict or str): Message to send
            topic_name (str): Topic to send to (uses default if None)
        """
        if not topic_name and not self.topic_name:
            raise Exception("No topic specified. Provide topic_name parameter or set default topic.")
            
        target_topic = topic_name or self.topic_name
        
        # Validate topic exists in configuration
        try:
            topic_config = self.config_service.get_topic_configuration(target_topic)
            self.logger.info(f"Sending to topic '{target_topic}' - Purpose: {topic_config['purpose']}")
        except ValueError:
            self.logger.warning(f"Topic '{target_topic}' not found in configuration, proceeding anyway")
        
        # Convert message to JSON string if it's a dict
        if isinstance(message, dict):
            message = json.dumps(message)
        
        try:
            self.producer.produce(target_topic, value=message)
            self.producer.flush()
            self.logger.info(f"Message sent to topic '{target_topic}': {message}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False
    
    def send_batch(self, messages, topic_name=None):
        """
        Send multiple messages in batch.
        
        Args:
            messages (list): List of messages to send
            topic_name (str): Topic to send to
        """
        if not topic_name and not self.topic_name:
            raise Exception("No topic specified. Provide topic_name parameter or set default topic.")
            
        target_topic = topic_name or self.topic_name
        successful_sends = 0
        
        for message in messages:
            if isinstance(message, dict):
                message = json.dumps(message)
            
            try:
                self.producer.produce(target_topic, value=message)
                successful_sends += 1
            except Exception as e:
                self.logger.error(f"Failed to send message in batch: {e}")
        
        self.producer.flush()
        self.logger.info(f"Sent {successful_sends}/{len(messages)} messages to '{target_topic}'")
        return successful_sends
    
    def close(self):
        """Close the producer connection."""
        self.producer.flush()
        self.logger.info("Producer closed")


class KafkaConsumer:
    """
    A class to consume (read) messages from Kafka topics.
    Designed for insurance data engineering workflows.
    """
    
    def __init__(self, environment='nonprod', topic_name=None, consumer_group=None, config_service=None):
        """
        Initialize the Kafka consumer.
        
        Args:
            environment (str): Environment name ('prod', 'nonprod', 'staging')
            topic_name (str): Topic to read from
            consumer_group (str): Consumer group name (uses configured group if available)
            config_service (ApplicationConfigurationService): Optional config service instance
        """
        self.environment = environment
        self.config_service = config_service or ApplicationConfigurationService()
        self.topic_name = topic_name
        self.logger = logging.getLogger(__name__)
        
        # Get Kafka configuration for the environment
        self.kafka_config = self.config_service.get_configuration(environment, 'kafka')
        
        # Resolve credentials from Databricks secrets
        self.credentials = self.config_service.resolve_secrets(
            environment, 
            'kafka', 
            self.kafka_config['credentials']
        )
        
        # Determine consumer group
        if consumer_group:
            try:
                group_config = self.config_service.get_consumer_group_configuration(consumer_group)
                self.group_id = consumer_group
                print(f"Using consumer group '{consumer_group}' - Purpose: {group_config['purpose']}")
                if self.topic_name and self.topic_name not in group_config['topics']:
                    print(f"Warning: Topic '{self.topic_name}' not typically used by group '{consumer_group}'")
            except ValueError:
                self.group_id = consumer_group
                print(f"Warning: Consumer group '{consumer_group}' not found in configuration, using as-is")
        else:
            self.group_id = self.kafka_config['default-group-id']
        
        # Validate topic if provided
        if self.topic_name:
            try:
                self.topic_config = self.config_service.get_topic_configuration(self.topic_name)
                print(f"Topic '{self.topic_name}' purpose: {self.topic_config['purpose']}")
            except ValueError as e:
                print(f"Warning: {e}")
                self.topic_config = None
        
        # Create consumer with configuration
        self.consumer = Consumer(self._get_consumer_config())
        
        if self.topic_name:
            self.consumer.subscribe([self.topic_name], on_assign=self._print_assignment)
        
        print(f"Kafka Consumer initialized for {self.environment} environment")
        print(f"  Bootstrap servers: {self.kafka_config['bootstrap-servers']}")
        print(f"  Group ID: {self.group_id}")
        print(f"  Secret scope: {self.kafka_config['secret-scope']}")
    
    def _get_token(self, config):
        """Get OAuth token for authentication."""
        payload = {
            'grant_type': 'client_credentials',
            'scope': 'kafka'
        }
        
        resp = requests.post(
            self.kafka_config['token-url'],
            auth=(self.credentials['client-id'], self.credentials['client-secret']),
            data=payload
        )
        
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])
    
    def _get_consumer_config(self):
        """Get consumer configuration."""
        return {
            'bootstrap.servers': self.kafka_config['bootstrap-servers'],
            'security.protocol': 'sasl_ssl',
            'sasl.mechanisms': 'OAUTHBEARER',
            'group.id': self.group_id,
            'ssl.ca.location': self.kafka_config['ssl-ca-location'],
            'oauth_cb': functools.partial(self._get_token),
            'logger': self.logger,
        }
    
    def _print_assignment(self, consumer, partitions):
        """Callback for partition assignment."""
        self.logger.info(f"Assignment: {partitions}")
    
    def subscribe_to_topic(self, topic_name):
        """
        Subscribe to a specific topic.
        
        Args:
            topic_name (str): Topic to subscribe to
        """
        # Validate topic
        try:
            topic_config = self.config_service.get_topic_configuration(topic_name)
            print(f"Subscribing to topic '{topic_name}' - Purpose: {topic_config['purpose']}")
        except ValueError:
            print(f"Warning: Topic '{topic_name}' not found in configuration, subscribing anyway")
        
        self.topic_name = topic_name
        self.consumer.subscribe([topic_name], on_assign=self._print_assignment)
        self.logger.info(f"Subscribed to topic: {topic_name}")
    
    def consume_messages(self, timeout=1.0, max_messages=None):
        """
        Consume messages from the topic.
        
        Args:
            timeout (float): Poll timeout in seconds
            max_messages (int): Maximum number of messages to consume (None for unlimited)
        
        Returns:
            list: List of consumed messages
        """
        if not self.topic_name:
            raise Exception("No topic subscribed. Call subscribe_to_topic() first or provide topic in constructor.")
            
        messages = []
        message_count = 0
        
        self.logger.info(f"Starting to consume from topic '{self.topic_name}'")
        
        try:
            while True:
                msg = self.consumer.poll(timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process the message
                message_value = msg.value().decode('utf-8')
                try:
                    parsed_message = json.loads(message_value)
                    messages.append(parsed_message)
                except json.JSONDecodeError:
                    messages.append(message_value)
                
                message_count += 1
                self.logger.info(f"Consumed message {message_count}: {message_value}")
                
                if max_messages and message_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            self.logger.info("Consumption interrupted by user")
        
        return messages
    
    def consume_single_message(self, timeout=5.0):
        """
        Consume a single message from the topic.
        
        Args:
            timeout (float): Poll timeout in seconds
        
        Returns:
            dict or str or None: The consumed message or None if no message
        """
        if not self.topic_name:
            raise Exception("No topic subscribed. Call subscribe_to_topic() first or provide topic in constructor.")
            
        msg = self.consumer.poll(timeout)
        
        if msg is None:
            return None
        
        if msg.error():
            self.logger.error(f"Consumer error: {msg.error()}")
            return None
        
        message_value = msg.value().decode('utf-8')
        try:
            return json.loads(message_value)
        except json.JSONDecodeError:
            return message_value
    
    def close(self):
        """Close the consumer connection."""
        self.consumer.close()
        self.logger.info("Consumer closed")


# Helper functions
def create_kafka_producer(environment, topic_name, config_service=None):
    """
    Quick helper function to create a Kafka producer
    
    Args:
        environment (str): Environment name
        topic_name (str): Default topic name
        config_service (ApplicationConfigurationService): Optional config service instance
    
    Returns:
        KafkaProducer: Configured producer
    """
    return KafkaProducer(environment, topic_name, config_service)


def create_kafka_consumer(environment, topic_name, consumer_group=None, config_service=None):
    """
    Quick helper function to create a Kafka consumer
    
    Args:
        environment (str): Environment name
        topic_name (str): Topic to subscribe to
        consumer_group (str): Consumer group name
        config_service (ApplicationConfigurationService): Optional config service instance
    
    Returns:
        KafkaConsumer: Configured consumer
    """
    return KafkaConsumer(environment, topic_name, consumer_group, config_service)   