from kafka import KafkaProducer
import json
import time
import pandas as pd

dataset = pd.read_csv('Synthetic_Financial_datasets_log.csv')

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for index, row in dataset.iterrows():
    transaction = row.to_dict()
    producer.send('transactions', transaction)
    print(f"Sent: {transaction}")
    time.sleep(1)

producer.close()