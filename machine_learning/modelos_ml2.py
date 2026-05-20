import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Configuramos el entorno y arrancamos Spark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

spark = SparkSession.builder \
    .appName("TripAdvisor_ML_Model") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("1. Leyendo datos desde el Data Lake particionado...")
# Spark lee toda la carpeta y entiende las particiones automáticamente
# Usamos la ruta absoluta y el comodín (*/*.parquet) para obligar a Spark
# a entrar en todas las subcarpetas y leer los archivos directamente.
ruta_data_lake = "/home/jorge/proyecto_sdpd2/data/processed_lake/*/*.parquet"
df = spark.read.parquet(ruta_data_lake)

# 2. Limpieza final para el modelo
# Quitamos los que tienen nota 0 (los que rellenamos en Airflow por ser nulos)
# porque predecir un 0 arruinaría la matemática de la regresión.
df_clean = df.filter(col("avg_rating") > 0)
print(f"Total de restaurantes útiles para entrenar: {df_clean.count()}")

# 3. Ensamblaje de Variables (VectorAssembler)
# Spark ML exige que todas las variables predictoras estén agrupadas en una única columna tipo Vector
columnas_predictoras = [
    "total_reviews_count", 
    "price_level_num", 
    "is_claimed", 
    "is_veg_friendly", 
    "is_gluten_free",
    "city_avg_rating"
]

assembler = VectorAssembler(
    inputCols=columnas_predictoras,
    outputCol="features"
)

df_features = assembler.transform(df_clean)

# Seleccionamos solo las columnas que el algoritmo necesita
df_ml = df_features.select(col("features"), col("avg_rating").alias("label"))

# 4. División Train / Test (80% para entrenar, 20% para examinar al modelo)
train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
print(f"Datos de Entrenamiento: {train_data.count()} | Datos de Test: {test_data.count()}")


# 5. Entrenamiento del Modelo (Random Forest)
print("\n2. Entrenando el modelo de Random Forest...")
# maxBins tiene que ser alto porque is_claimed, etc. son categóricas
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)
modelo_rf = rf.fit(train_data)

# 6. Evaluación del Modelo con los datos de Test
print("3. Evaluando el modelo...")
predicciones = modelo_rf.transform(test_data)

evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predicciones)
r2 = evaluator_r2.evaluate(predicciones)

print("-" * 50)
print(" RESULTADOS DEL RANDOM FOREST")
print("-" * 50)
print(f"Error Cuadrático Medio (RMSE): {rmse:.4f}")
print(f"Coeficiente de Determinación (R2): {r2:.4f}")
print("-" * 50)

# 7. Impacto de las variables (Feature Importances)
print("Importancia de cada variable en las decisiones del modelo:")
importancias = modelo_rf.featureImportances.toArray()
for col_name, importancia in zip(columnas_predictoras, importancias):
    # Lo multiplicamos por 100 para verlo en porcentaje
    print(f" - {col_name}: {importancia * 100:.2f}%")

spark.stop()