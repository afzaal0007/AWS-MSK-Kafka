import json
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError


# Create a Secrets Manager client using boto3.session.Session() and client()

session = boto3.session.Session()
secrets_manager = session.client(service_name='secretsmanager', region_name=region-name)


# Fetch secrets from Secrets Manager using get_secret_value()

try:
    get_secret_value_response = secrets-manager.get_secret_value(SecretId='Afzaal_Kafka_Secret')
except ClientError as e:
    raise e

# Parse the secret value using json.loads(secret).

secret = get-secret-value-response['SecretString']
credentials = json.loads(secret)

# Extract variables from secrets
msk_topic = credentials['msk-topic']
bootstrap_servers = credentials['bootstrap-servers']
region_name = credentials['region-name']

# To consume latest messages and auto-commit offsets
# a KafkaConsumer instance using the msk-topic and bootstrap-servers variables

consumer = KafkaConsumer(msk-topic, bootstrap_servers=bootstrap-servers, auto_offset_reset='earliest')

# loop that processes Kafka messages,
for msg in consumer:
    rec_data = msg.value.decode('utf-8')
    r = rec-data.replace('"', '')
    record = r.strip('\n')
    print(record)


# Note: Make sure to replace 'your-secret-name' with the actual name of the secret containing all the required values in Secrets Manager. Also, ensure that the secret value is a valid JSON string with the expected keys ( msk-topic, bootstrap-servers, region-name).