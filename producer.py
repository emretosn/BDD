import yfinance as yf
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Fetch data from Yahoo Finance
def fetch_data(symbol):
    data = yf.download(tickers=symbol, period='1d', interval='1m')
    return data.to_dict(orient="records")

# Send data to Kafka
def send_to_kafka(topic, data):
    for record in data:
        producer.send(topic, record)
        producer.flush()

def continuous_fetch_and_send(symbol, topic):
    while True:
        data = fetch_data(symbol)
        send_to_kafka(topic, data)
        time.sleep(60)  # Yahoo Finance allows every minute

# Start the process for a symbol, e.g., AAPL
continuous_fetch_and_send('AAPL', 'yahoo-finance')
