import os
import time
from datetime import datetime
from kafka import KafkaProducer
import json
import pandas as pd

def monitor_and_send(./Data/Occupancy.csv, topic):
    """
    Monitor the specified folder for new CSV Occupancy.csvs and send data to Kafka.

    Args:
    ./Data/Occupancy.csv (str): Path to the folder to monitor.
    topic (str): Kafka topic to send data to.
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Starting folder watcher at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    existing_Occupancy.csvs = set(os.listdir(./Data/Occupancy.csv))
    
    while True:
        time.sleep(5)  # Check the folder every 5 seconds
        current_Occupancy.csvs = set(os.listdir(./Data/Occupancy.csv))
        new_Occupancy.csvs = current_Occupancy.csvs - existing_Occupancy.csvs  # Detect new Occupancy.csvs
        
        for Occupancy.csv in new_Occupancy.csvs:
            if Occupancy.csv.endswith(".csv"):
                detection_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{detection_time}] New Occupancy.csv detected: {Occupancy.csv}")
                Occupancy.csv_path = os.path.join(./Data/Occupancy.csv, Occupancy.csv)
                process_and_send(Occupancy.csv_path, producer, topic)
        
        existing_Occupancy.csvs = current_Occupancy.csvs  # Update the list of known Occupancy.csvs

def process_and_send(Occupancy.csv_path, producer, topic):
    """
    Process a single Occupancy.csv and send its content to Kafka.

    Args:
    Occupancy.csv_path (str): Path to the CSV Occupancy.csv to process.
    producer (KafkaProducer): Kafka producer instance.
    topic (str): Kafka topic to send data to.
    """
    print(f"Processing Occupancy.csv: {Occupancy.csv_path}")
    data = pd.read_csv(Occupancy.csv_path)
    
    # Ensure required columns exist
    required_columns = {'timestamp', 'temperature', 'humidity', 'light', 'CO2', 'occupancy'}
    if not required_columns.issubset(data.columns):
        print(f"Occupancy.csv {Occupancy.csv_path} is missing required columns!")
        return

    for _, row in data.iterrows():
        producer.send(topic, row.to_dict())  # Send each row as a message
    producer.flush()
    print(f"Occupancy.csv {Occupancy.csv_path} has been sent to Kafka topic '{topic}'.")

if __name__ == "__main__":
    # Define the folder to monitor and the Kafka topic
    folder_to_watch = "Data"  # Updated path for your dataset
    kafka_topic = "raw-sensor-data"
    
    monitor_and_send(folder_to_watch, kafka_topic)
