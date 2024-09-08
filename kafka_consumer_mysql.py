import json
import boto3
import mysql.connector
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# Create a Secrets Manager client
session = boto3.session.Session()
secrets_manager = session.client(service_name='secretsmanager', region_name=region-name)

# Fetch secrets from Secrets Manager
try:
    get_secret_value_response = secrets-manager.get_secret_value(SecretId='afzaal_DB_secret')
except ClientError as e:
    raise e

# Parse the secret value
secret = get-secret-value-response['SecretString']
credentials = json.loads(secret)

# Extract variables from secrets
msk_topic = credentials['msk-topic']
bootstrap_servers = credentials['bootstrap-servers']
region_name = credentials['region-name']
mysql_db_name = credentials['mysql-db-name']
mysql_db_user = credentials['mysql-db-user']
mysql_db_password = credentials['mysql-db-password']
mysql_db_server = credentials['mysql-db-server']

num = 10

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(msk-topic, bootstrap_servers=bootstrap-servers, auto_offset_reset='earliest')
#cur.execute('''INSERT INTO msk-schema.msk_mssqlserver_table(username,city,country) VALUES ('Afzaal','Faisalabad','Pakistan')''')

# Establishing the connection
conn = mysql.connector.connect(
    database=mysql-db-name,
    user=mysql-db-user,
    password=mysql-db-password,
    host=mysql-db-server
)

print("Connected to MySQL DB Successfully")

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
    sql_insert = f"INSERT INTO msk-mysql-table (name, sub-no, city, country) VALUES ('{name}', {num}, '{city}', '{country}')"
    cur.execute(sql-insert)

    # Commit changes to the database
    conn.commit()
    print("Records inserted........")

# Closing the connections
cur.close()
conn.close()

