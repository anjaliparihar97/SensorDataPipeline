from kafka import KafkaConsumer
from sqlalchemy import create_engine
import pandas as pd
import json

def process_and_store():
    """
    Consume data from Kafka, process it, and store it in PostgreSQL.
    """
    consumer = KafkaConsumer(
        'raw-sensor-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    engine = create_engine('postgresql://username:password@localhost:5432/sensor_data')

    for message in consumer:
        record = message.value  # Read data from Kafka
        print(f"Processing record: {record}")
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame([record])
        
        # Write raw data to the database
        df.to_sql('raw_sensor_data', engine, if_exists='append', index=False)

        # Compute aggregated metrics
        aggregated = {
            'avg_temperature': df['temperature'].mean(),
            'avg_humidity': df['humidity'].mean(),
            'avg_light': df['light'].mean(),
            'avg_CO2': df['CO2'].mean(),
            'occupancy_rate': df['occupancy'].mean(),  # Proportion of time occupied
            'timestamp': df['timestamp'].iloc[0]  # Use first timestamp as reference
        }

        # Write aggregated metrics to the database
        df_agg = pd.DataFrame([aggregated])
        df_agg.to_sql('processed_sensor_data', engine, if_exists='append', index=False)
        print(f"Aggregated data stored: {aggregated}")

if __name__ == "__main__":
    process_and_store()
