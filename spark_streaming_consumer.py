"""
Consumidor Spark Structured Streaming - Trafico aereo en tiempo real.

Basado en spark_streaming_consumer.py del Anexo 3: lee el topic "open_sky_data" de Kafka,
interpreta el JSON de cada evento y calcula estadisticas por ventana de 1 minuto y pais
de origen de la aeronave. Los resultados se muestran en la consola.

Ejecucion (primero el productor, luego este script):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               BooleanType, TimestampType)
from Constantes.constantes import Constantes

# Sesion de Spark (el Anexo 3 la crea dos veces; con una es suficiente)
spark = SparkSession.builder \
    .appName("AirTrafficStreaming") \
    .getOrCreate()
# Nivel de log WARN para reducir los mensajes INFO
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos de entrada (mismos campos que envia kafka_producer.py).
# timestamp llega como epoch en segundos y Spark lo convierte a TimestampType.
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("pais", StringType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("altitud_m", DoubleType()),
    StructField("velocidad_kmh", DoubleType()),
    StructField("en_tierra", BooleanType()),
    StructField("timestamp", TimestampType()),
])

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", Constantes.PORT) \
    .option("subscribe", Constantes.KAFKA_TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# Parsear los datos JSON
parsed_df = df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# Estadisticas por ventana de 1 minuto y pais de origen
windowed_stats = parsed_df \
    .groupBy(F.window(F.col("timestamp"), "1 minute"), F.col("pais")) \
    .agg(
        F.approx_count_distinct("icao24").alias("aeronaves"),
        F.count("*").alias("lecturas"),
        F.round(F.avg("velocidad_kmh"), 1).alias("vel_prom_kmh"),
        F.round(F.max("velocidad_kmh"), 1).alias("vel_max_kmh"),
        F.round(F.avg("altitud_m"), 0).alias("alt_prom_m"),
    ) \
    .orderBy(F.col("window").desc(), F.col("aeronaves").desc())

# Escribir los resultados en la consola (modo complete: tabla completa en cada lote)
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
