from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
from configs import kafka_config
import sys

my_name = "oleksbod"

topics = [
    f"{my_name}_building_sensors",
    f"{my_name}_alerts"  # Один топік для всіх алертів
]

print(f"Connecting to Kafka broker at {kafka_config['bootstrap_servers']}...")

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']        
    )
    print("Connected successfully!")
except NoBrokersAvailable as e:
    print(f"\n ERROR: Cannot connect to Kafka broker!")    
    sys.exit(1)
except Exception as e:
    print(f"\n ERROR: Failed to create admin client: {e}")
    sys.exit(1)

try:
    print(f"\nCreating topics: {', '.join(topics)}")
    new_topics = [NewTopic(name=t, num_partitions=2, replication_factor=1) for t in topics]
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Topics created successfully!")
except Exception as e:
    if "TopicExistsException" in str(type(e).__name__) or "already exists" in str(e).lower():
        print(f"Warning: Some topics may already exist: {e}")
        print("Continuing to list existing topics...")
    else:
        print(f"Error creating topics: {e}")
        admin_client.close()
        sys.exit(1)

try:
    print("\nMy Existing topics:")
    existing_topics = [topic for topic in admin_client.list_topics() if my_name in topic]
    if existing_topics:
        for topic in existing_topics:
            print(f"  - {topic}")
    else:
        print("No my topics found.")
except Exception as e:
    print(f" Warning: Could not list topics: {e}")

admin_client.close()
print("\n Script completed successfully!")
