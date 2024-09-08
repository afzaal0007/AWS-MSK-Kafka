import json  # JSON module to parse the secret value from Secrets Manager.
import sys
import boto3 # to interact with AWS Services
import pymssql
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# Create a Secrets Manager client
session = boto3.session.Session()
secrets_manager = session.client(service_name='secretsmanager', region_name=region_name)

# Fetch secrets from Secrets Manager
try:
    get_secret_value_response = secrets_manager.get_secret_value(SecretId='Afzaal_Kafka_Secret')
except ClientError as e:
    raise e

# Parse the secret value
secrets = json.loads(get_secret_value_response['SecretString'])

# Assumed that the secrets value are JSON string containing the secret value i.e  valid JSON string with the expected keys (msk_topic, bootstrap_servers, region_name, mssqlserver_db_name, mssqlserver_db_user, mssqlserver_db_password, mssqlserver_db_server) and parsed it using json.loads()

credentials = json.loads(secret)

# Extract variables from secrets
msk_topic = secrets['msk_topic']
bootstrap_servers = secrets['bootstrap_servers']
region_name = secrets['region_name']
mssqlserver_db_name = secrets['mssqlserver_db_name']
mssqlserver_db_user = secrets['mssqlserver_db_user']
mssqlserver_db_password = secrets['mssqlserver_db_password']
mssqlserver_db_server = secrets['mssqlserver_db_server']

num = 10

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(msk_topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

# Establishing the connection to the MS SQL Server database
conn = pymssql.connect(
    database=mssqlserver_db_name,
    user=mssqlserver_db_user,
    password=mssqlserver_db_password,
    server=mssqlserver_db_server
)

print("Connected to MSSQL mssqlserver_db Successfully")

# Create a cursor object
cur = conn.cursor()

for msg in consumer:
    num = num + 1
    rec_data = msg.value.decode('utf-8')
    r = rec_data.replace('"', '')
    record = r.strip('\n')

    f_rec = record.split(",")
    name = f_rec[0]
    city = f_rec[1]
    country = f_rec[2]

    print(name)
    print(num)
    print(city)
    print(country)
    print("-----------")

    # Execute SQL INSERT statement
    sql_insert = f"INSERT INTO msk-mssqlserver-table (name, city, country) VALUES ('{name}', '{city}', '{country}')"
    #cur.execute('''INSERT INTO msk-schema.msk_mssqlserver_table(username,city,country) VALUES ('Afzaal','Faisalabad','Pakistan')''')

    cur.execute(sql_insert)

    # Commit changes to the database
    conn.commit()
    print("Records inserted........")

# Closing the connections
cur.close()
conn.close()
