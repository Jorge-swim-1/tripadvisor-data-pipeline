from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

import os
# Configuramos la variable de entorno directamente desde Python
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# ---------------------------------------------------------
# 1. INICIALIZAR SPARK SESSION
# ---------------------------------------------------------
# Importante: Incluimos el paquete de Kafka necesario para la conexión
spark = SparkSession.builder \
    .appName("TripAdvisorStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Reducimos los logs para que la consola esté limpia y las capturas de pantalla queden bien
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# 2. DEFINIR EL ESQUEMA (StructType)
# ---------------------------------------------------------
tripadvisor_schema = StructType([
    StructField("restaurant_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("avg_rating", DoubleType(), True),
    StructField("city_avg_rating", DoubleType(), True),
    StructField("rating_diff_city", DoubleType(), True),
    StructField("total_reviews_count", IntegerType(), True), 
    StructField("price_level_num", IntegerType(), True),
    StructField("is_claimed", IntegerType(), True),
    StructField("is_veg_friendly", IntegerType(), True),
    StructField("is_gluten_free", IntegerType(), True)
])

print("Iniciando la conexión con Kafka en localhost:9092...")

# ---------------------------------------------------------
# 3. LEER EL FLUJO DESDE KAFKA (FASE DE INGESTA)
# ---------------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tripadvisor_restaurants") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. PARSEAR EL JSON BINARIO A COLUMNAS TABULARES
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), tripadvisor_schema).alias("data")) \
    .select("data.*")


# =========================================================
# BLOQUE DE EJECUCIÓN DE CONSULTAS (STREAMING)
# =========================================================

# ---> CONSULTA 0: Mostrar datos RAW (Ideal para tu primera captura de pantalla)
query_raw = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("raw_data") \
    .start()

# ---> CONSULTA 1: Filtrado (Sin gluten y Rating >= 4.5) - MODO APPEND
consulta1_df = parsed_df.filter((col("is_gluten_free") == 1) & (col("avg_rating") >= 4.5))

# Exporta los resultados a una carpeta 'salida1' en formato CSV (texto plano)
query1 = consulta1_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./salida1_txt") \
    .option("checkpointLocation", "./checkpoints/consulta1") \
    .start()

# ---> CONSULTA 2: Agregación (Conteo por país) - MODO COMPLETE
consulta2_df = parsed_df.groupBy("country").count()

# Nota: El modo complete requiere ver toda la tabla a la vez. Lo sacamos por consola
# para que podáis hacerle captura a cómo cambian los números en tiempo real.
query2 = consulta2_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("conteo_paises") \
    .start()

# ---------------------------------------------------------
# 5. MANTENER EL SCRIPT VIVO (Imprescindible en Streaming)
# ---------------------------------------------------------
spark.streams.awaitAnyTermination()