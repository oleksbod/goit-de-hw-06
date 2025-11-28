from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
import sys

try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8') if v else None
    )

    my_name = "oleksbod"
    topic_name = f"{my_name}_building_sensors"

    sensor_id = str(uuid.uuid4())
    
    # Вибір типу алерту 
    alert_type = None
    if len(sys.argv) > 1:
        try:
            alert_type = int(sys.argv[1])
            if alert_type not in [101, 102, 103, 104]:
                print(f"Невірний тип алерту: {alert_type}")
                print("   Доступні типи: 101, 102, 103, 104")
                print("   Використання: python sensor_producer.py [101|102|103|104]")
                print("   Якщо не вказано - генерує всі типи по черзі")
                alert_type = None
        except ValueError:
            print(f"Невірний формат: {sys.argv[1]}")
            print("   Використання: python sensor_producer.py [101|102|103|104]")
            alert_type = None
    
    print(f"Sensor started → ID: {sensor_id}")
    
    if alert_type:
        alert_names = {
            101: "humidity 0-40% (too dry)",
            102: "humidity 65-100% (too wet)",
            103: "temperature <= 32°C (too cold)",
            104: "temperature >= 38°C (too hot)"
        }
        print(f"TEST MODE: Генерація тільки Alert {alert_type} - {alert_names[alert_type]}")
    else:
        print("NORMAL MODE: Генерація всіх типів алертів по черзі")
        print("   Використання: python sensor_producer.py [101|102|103|104] для тестування одного типу")
    print()
    
    counter = 0

    while True:
       
        if alert_type == 101:
            # Alert 101: humidity 0-40 (too dry) + нормальна температура
            temperature = random.randint(31, 39)  # Нормальна температура
            humidity = random.randint(15, 40)  # Суха вологість
            print(f"🔵 Alert 101: humidity={humidity}% (too dry)")
        elif alert_type == 102:
            # Alert 102: humidity 65-100 (too wet) + нормальна температура
            temperature = random.randint(31, 39)  # Нормальна температура
            humidity = random.randint(65, 85)  # Висока вологість
            print(f"🟢 Alert 102: humidity={humidity}% (too wet)")
        elif alert_type == 103:
            # Alert 103: temperature <= 32 (too cold) + нормальна вологість
            temperature = random.randint(25, 32)  # Низька температура
            humidity = random.randint(36, 69)  # Нормальна вологість
            print(f"🔵 Alert 103: temperature={temperature}°C (too cold)")
        elif alert_type == 104:
            # Alert 104: temperature >= 38 (too hot) + нормальна вологість
            temperature = random.randint(38, 45)  # Висока температура
            humidity = random.randint(36, 69)  # Нормальна вологість
            print(f"🔴 Alert 104: temperature={temperature}°C (too hot)")
        else:
            # Циклічна генерація всіх типів (якщо не вибрано конкретний)
            current_alert = counter % 4
            if current_alert == 0:
                temperature = random.randint(31, 39)
                humidity = random.randint(15, 40)
                print(f"🔵 Alert 101: humidity={humidity}% (too dry)")
            elif current_alert == 1:
                temperature = random.randint(31, 39)
                humidity = random.randint(65, 85)
                print(f"🟢 Alert 102: humidity={humidity}% (too wet)")
            elif current_alert == 2:
                temperature = random.randint(25, 32)
                humidity = random.randint(36, 69)
                print(f"🔵 Alert 103: temperature={temperature}°C (too cold)")
            else:  # current_alert == 3
                temperature = random.randint(38, 45)
                humidity = random.randint(36, 69)
                print(f"🔴 Alert 104: temperature={temperature}°C (too hot)")
            counter += 1
        
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": temperature,
            "humidity": humidity
        }

        producer.send(topic_name, value=data)
        producer.flush()

        print(f"Sent → {data}")
        time.sleep(2)  
except KeyboardInterrupt:
    print("\nSensor stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'producer' in locals():
        producer.close()
