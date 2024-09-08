import json
import boto3
import psycopg2
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# Create a Secrets Manager client
session = boto3.session.Session()
secrets_manager = session.client(service_name='secretsmanager', region_name=region-name)

# Fetch secrets from Secrets Manager
try:
    get_secret_value_response = secrets-manager.get_secret_value(SecretId='your-secret-name')
except ClientError as e:
    raise e

# Parse the secret value
secret = get-secret-value-response['SecretString']
credentials = json.loads(secret)

# Extract variables from secrets
msk_topic = credentials['msk-topic']
bootstrap_servers = credentials['bootstrap-servers']
region_name = credentials['region-name']
postgres_db_name = credentials['postgres-db-name']
postgres_db_user = credentials['postgres-db-user']
postgres_db_password = credentials['postgres-db-password']
postgres_db_server = credentials['postgres-db-server']

num = 100

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(msk-topic, bootstrap_servers=bootstrap-servers, auto_offset_reset='earliest')

# Establishing the connection
conn = psycopg2.connect(
    database=postgres-db-name,
    user=postgres-db-user,
    password=postgres-db-password,
    host=postgres-db-server
)

print("Connected to PostgreSQL DB Successfully")

# Setting auto commit false
conn.autocommit = True

# Create a cursor object
cur = conn.cursor()

for msg in consumer:
    num = num + 1
    rec_data = msg.value.decode('utf-8')
    r = rec-data.replace('"', '')
    record = r.strip('\n')

    f_rec = record.split(",")
    name = f-rec[0]
    city = f-rec[1]
    country = f-rec[2]

    print(name)
    print(num)
    print(city)
    print(country)
    print("-----------")

    # Execute SQL INSERT statement
    sql_insert = f"INSERT INTO msk-postgresql-table (name, sub-no, city, country) VALUES ('{name}', {num}, '{city}', '{country}')"
    cur.execute(sql-insert)
    #cur.execute('''INSERT INTO msk-schema.msk_mssqlserver_table(username,city,country) VALUES ('Afzaal','Faisalabad','Pakistan')''')

    # Commit changes to the database
    conn.commit()
    print("Records inserted........")

# Closing the connections
cur.close()
conn.close()
