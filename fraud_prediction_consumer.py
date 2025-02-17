from kafka import KafkaConsumer
import json
import pandas as pd
import joblib

# Load the trained XGBoost model
model = joblib.load("xgboost_fraud_model.pkl")

# Kafka Consumer Configuration
KAFKA_TOPIC = "transaction_data"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

# Real-Time Prediction
print("Listening to Kafka topic for real-time predictions...")
for message in consumer:
    # Get the transaction data
    transaction = message.value
    
    # Convert to DataFrame
    df = pd.DataFrame([transaction])
    
    # Feature Engineering (matching training process)
    df["balanceOrgDiff"] = df["newbalanceOrig"] - df["oldbalanceOrg"]
    df["balanceDestDiff"] = df["newbalanceDest"] - df["oldbalanceDest"]
    df["amount_ratio"] = df["amount"] / df["oldbalanceOrg"].replace(0, 1)
    
    # One-hot encoding for 'type' column
    df = pd.get_dummies(df, columns=["type"], drop_first=True)
    
    # Ensure columns match the trained model
    feature_columns = ["step", "amount", "balanceOrgDiff", "balanceDestDiff", "amount_ratio"] + \
                      [col for col in df.columns if col.startswith("type_")]
    df = df.reindex(columns=feature_columns, fill_value=0)
    
    # Predict fraud
    prediction = model.predict(df)
    prediction_proba = model.predict_proba(df)[:, 1]
    
    # Output the result
    print(f"Transaction ID: {transaction['step']}, Fraud: {bool(prediction[0])}, Probability: {prediction_proba[0]:.2f}")