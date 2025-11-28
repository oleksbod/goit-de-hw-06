from kafka import KafkaConsumer
from configs import kafka_config
import json

my_name = "oleksbod"
alert_topic = f"{my_name}_alerts"

consumer = KafkaConsumer(
    alert_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f"{my_name}_alert_reader"
)

print(f"Listening for alerts from topic: {alert_topic}\n")
print("=" * 60)

try:
    for message in consumer:
        try:
            alert_data = message.value
            
            print(f"ALERT RECEIVED")
            print(f"  Sensor ID: {alert_data.get('sensor_id', 'N/A')}")
            print(f"  Timestamp: {alert_data.get('timestamp', 'N/A')}")
            print(f"  Temperature: {alert_data.get('temperature', 'N/A')}°C")
            print(f"  Humidity: {alert_data.get('humidity', 'N/A')}%")
            print(f"  Alert Code: {alert_data.get('alert', 'N/A')}")
            print(f"  Message: {alert_data.get('message', 'N/A')}")
            print("-" * 60)
        except Exception as e:
            print(f"Error processing alert: {e}")
            continue
except KeyboardInterrupt:
    print("\nAlert consumer stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'consumer' in locals():
        consumer.close()
