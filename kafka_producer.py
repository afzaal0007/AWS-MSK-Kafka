import json
import boto3
from kafka import KafkaProducer
from botocore.exceptions import ClientError

# Create a Secrets Manager client
session = boto3.session.Session()
secrets_manager = session.client(service_name='secretsmanager', region_name=region-name)

# Fetch secrets from Secrets Manager
try:
    get_secret_value_response = secrets_manager.get_secret_value(SecretId='Afzaal_Kafka_Secret')
except ClientError as e:
    raise e

# Parse the secret value
secret = get-secret-value-response['SecretString']
credentials = json.loads(secret)

# Extract variables from secrets
msk_topic = credentials['msk-topic']
bootstrap_servers = credentials['bootstrap-servers']
region_name = credentials['region-name']

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Open the file and read data
with open('data.csv', 'r') as file:
    for line in file:
        data = line.strip()
        
        # Send the data to the Kafka topic
        producer.send(msk_topic, value=data.encode('utf-8'))
        sleep(2)
