import os
import sys

hadoop_dir = os.path.join(os.getcwd(), 'hadoop')
if not os.path.exists(hadoop_dir):
    os.makedirs(hadoop_dir, exist_ok=True)

os.environ['HADOOP_HOME'] = hadoop_dir
os.environ['HADOOP_CONF_DIR'] = hadoop_dir

if sys.platform == 'win32':    
    winutils_path = os.path.join(hadoop_dir, 'bin', 'winutils.exe')
    if os.path.exists(winutils_path):
        os.environ['PATH'] = os.path.join(hadoop_dir, 'bin') + os.pathsep + os.environ.get('PATH', '')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configs import kafka_config
import pandas as pd

# Add Kafka packages
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages '
    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 '
    'pyspark-shell'
)

my_name = "oleksbod"
input_topic = f"{my_name}_building_sensors"
output_topic = f"{my_name}_alerts"

spark_builder = SparkSession.builder \
    .appName("IoT Alert Processor") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.mapreduce.framework.name", "local")

# Додаткові конфігурації для Windows
if sys.platform == 'win32':
    spark_builder = spark_builder \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = spark_builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Зчитування умов алертів з CSV
csv_path = os.path.join("data", "alerts_conditions.csv")
alerts_df = pd.read_csv(csv_path)
alerts_spark_df = spark.createDataFrame(alerts_df)

print(f"Loaded {alerts_spark_df.count()} alert conditions from {csv_path}")
alerts_spark_df.show()

# Схема вхідних даних
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Зчитування потоку з Kafka
# maxOffsetsPerTrigger обмежує кількість повідомлень за batch для швидшої обробки
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{kafka_config['username']}\" "
            f"password=\"{kafka_config['password']}\";") \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "500") \
    .load()

# --- Parse JSON ---
# Kafka зберігає value як bytes
parsed_stream = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_string")
    .withColumn("value_json", from_json(col("json_string"), schema))
    .select(
        col("value_json.sensor_id").alias("sensor_id"),
        col("value_json.timestamp").alias("timestamp"),
        col("value_json.temperature").alias("temperature"),
        col("value_json.humidity").alias("humidity")
    )
    .filter(col("sensor_id").isNotNull())  # Фільтруємо NULL значення (невалідний JSON)
)

# Конвертація timestamp
stream_with_time = parsed_stream.withColumn(
    "event_time", 
    from_unixtime(col("timestamp")).cast("timestamp")
)

# Агрегація з sliding window
# Window: 1 хвилина, sliding interval: 30 секунд, watermark: 10 секунд
windowed_agg = stream_with_time \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        max("event_time").alias("window_end")
    )

# Cross join з умовами алертів
alerts_broadcast = broadcast(alerts_spark_df)
cross_joined = windowed_agg.crossJoin(alerts_broadcast)

# Фільтрація алертів
# -999 означає, що умова не використовується
# Перевіряємо середні значення на відповідність умовам алертів
alerts = cross_joined.filter(
    # Temperature: якщо min != -999, то avg >= min; якщо max != -999, то avg <= max
    ((col("temperature_min") == -999) | (col("avg_temperature") >= col("temperature_min"))) &
    ((col("temperature_max") == -999) | (col("avg_temperature") <= col("temperature_max"))) &
    # Humidity: якщо min != -999, то avg >= min; якщо max != -999, то avg <= max
    ((col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min"))) &
    ((col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max"))) &
    # Принаймні одна умова має бути активною (не -999)
    ~(
        (col("temperature_min") == -999) & 
        (col("temperature_max") == -999) & 
        (col("humidity_min") == -999) & 
        (col("humidity_max") == -999)
    )
)

# Формування повідомлення алерту
alert_output = alerts.select(
    col("sensor_id"),
    col("window_end").alias("timestamp"),
    col("avg_temperature").alias("temperature"),
    col("avg_humidity").alias("humidity"),
    col("code").alias("alert"),
    col("message")
)

# Конвертація в JSON для запису в Kafka
alert_json = alert_output.select(
    to_json(struct([alert_output[x] for x in alert_output.columns])).alias("value")
)

# Запис в Kafka
query = alert_json \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("topic", output_topic) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username=\"{kafka_config['username']}\" "
            f"password=\"{kafka_config['password']}\";") \
    .option("checkpointLocation", os.path.abspath("./checkpoint").replace("\\", "/")) \
    .trigger(processingTime="5 seconds") \
    .outputMode("append") \
    .start()

print("\n============================================================")
print(f"Processing stream: {input_topic} → {output_topic}")
print("Window: 1 minute | Slide: 30 seconds | Watermark: 10 seconds")
print("Trigger: 5 seconds | Max offsets per trigger: 500")
print("============================================================\n")

# Діагностика статусу query
print(f"\nQuery ID: {query.id}")
print(f"Query Name: {query.name}")
print(f"Query Status: {query.status}")
print(f"Is Active: {query.isActive}")

# Моніторинг прогресу в циклі
import threading
import time

def monitor_progress():
    """Моніторинг прогресу query"""
    last_batch_id = -1
    wait_count = 0
    while query.isActive:
        try:
            progress = query.lastProgress
            if progress:
                batch_id = progress.get('batchId', -1)
                input_rows = progress.get('inputRowsPerSecond', 0)
                state = progress.get('state', 'N/A')
                sources = progress.get('sources', [])
                
                # Перевірка, чи є новий batch
                if batch_id > last_batch_id:
                    print(f"\n📊 Batch {batch_id}: Input rows/sec: {input_rows}, State: {state}")
                    if sources:
                        for source in sources:
                            start_offset = source.get('startOffset', {})
                            end_offset = source.get('endOffset', {})
                            num_input_rows = source.get('numInputRows', 0)
                            description = source.get('description', 'N/A')
                            print(f"   📥 Kafka Source: {description}")
                            print(f"   📊 Input rows: {num_input_rows}")
                            if start_offset or end_offset:
                                print(f"   📍 Start offsets: {start_offset}")
                                print(f"   📍 End offsets: {end_offset}")
                            if num_input_rows == 0:
                                print("   ⚠️  WARNING: 0 rows processed!")                             
                    
                    # Перевірка sink (Kafka output) - чи записуються алерти
                    sinks = progress.get('sink', {})
                    if sinks:
                        num_output_rows = sinks.get('numOutputRows', 0)
                        if num_output_rows > 0:
                            print(f"   ✅ Alerts written to Kafka: {num_output_rows} alerts")
                        else:
                            print(f"   ⚠️  No alerts written (0 rows)")                           
                    
                    last_batch_id = batch_id
                    wait_count = 0
                else:
                    # Якщо batch той самий, але є прогрес - показуємо статус
                    if batch_id >= 0:
                        print(f"⏳ Processing Batch {batch_id}... (State: {state})")
            else:
                # Показуємо різні повідомлення залежно від того, скільки часу чекаємо
                wait_count += 1
                if wait_count <= 3:
                    print("⏳ Waiting for data from Kafka...")
                elif wait_count == 4:
                    print("⏳ Still waiting... (перевірте, чи запущений sensor_producer.py)")                
            time.sleep(5)
        except Exception as e:
            if query.isActive:
                print(f"⚠️  Monitor error: {e}")
            break

# Запуск моніторингу в окремому потоці
monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
monitor_thread.start()

print("\n🚀 Query started. Waiting for data from Kafka...")
print("💡 TIP: Запустіть sensor_producer.py в іншому терміналі для генерації даних\n")

query.awaitTermination()
