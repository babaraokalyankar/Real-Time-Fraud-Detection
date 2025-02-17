from kafka import KafkaConsumer
import json
import psycopg2

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

connection = psycopg2.connect(
    dbname="fraud_detection",
    user="user",
    password="password",
    host="postgres",
    port="5432"
)
cursor = connection.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    step INT,
    type TEXT,
    amount FLOAT,
    nameOrig TEXT,
    oldbalanceOrg FLOAT,
    newbalanceOrig FLOAT,
    nameDest TEXT,
    oldbalanceDest FLOAT,
    newbalanceDest FLOAT,
    isFraud INT,
    isFlaggedFraud INT
)
""")
connection.commit()

for message in consumer:
    transaction = message.value
    cursor.execute("""
    INSERT INTO transactions (step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        transaction['step'],
        transaction['type'],
        transaction['amount'],
        transaction['nameOrig'],
        transaction['oldbalanceOrg'],
        transaction['newbalanceOrig'],
        transaction['nameDest'],
        transaction['oldbalanceDest'],
        transaction['newbalanceDest'],
        transaction['isFraud'],
        transaction['isFlaggedFraud']
    ))
    connection.commit()
    print(f"Inserted: {transaction}")

cursor.close()
connection.close()