# 🍽️ Predicción de Éxito en TripAdvisor: Pipeline ETL, Streaming y Machine Learning

**Universidad:** Universidad Rey Juan Carlos (URJC)  
**Asignatura:** Sistemas Distribuidos de Procesamiento de Datos II (SDPD2) - Grado en Ciencia e Ingeniería de Datos  
**Curso:** 2025/2026  
**Autores:** Jorge de Dios Orellana | Rafael Cañas  

---

## 🎯 Descripción del Proyecto
Este proyecto resuelve un problema analítico de negocio: **¿Podemos predecir la valoración media (`avg_rating`) que obtendrá un nuevo restaurante en Europa antes de su apertura?** Para responder a esta pregunta, hemos diseñado e implementado una arquitectura Big Data distribuida de extremo a extremo que procesa casi un millón de registros. El ecosistema abarca la orquestación ETL, la ingesta de eventos en streaming y el modelado predictivo distribuido.

## 🏗️ Arquitectura Tecnológica y Fases
1. **Fase 1: Orquestación ETL (Apache Airflow + Polars)**
   - Extracción, validación estricta y Feature Engineering vectorizado.
   - *Patrones Avanzados:* Prevención de Data Leakage, Window Functions (contexto competitivo), Dead Letter Queue (DLQ) para anomalías, y Data Lake particionado por país con PyArrow.
   - *Idempotencia:* Offset Watermarking local para evitar duplicación de eventos en el origen.
2. **Fase 2: Consumo Streaming (Apache Kafka + Spark Structured Streaming)**
   - Deserialización binaria en memoria con esquema estricto (`StructType`).
   - Consultas en tiempo real operando en **Modo Append** (filtrado a CSV) y **Modo Complete** (agregación en consola).
3. **Fase 3: Modelado Predictivo ML (Apache Spark MLlib)**
   - Entrenamiento en modo Batch sobre los datos estacionarios.
   - Comparativa algorítmica: Regresión Lineal vs Random Forest Regressor (con análisis crítico del negocio e interpretabilidad de pesos).

---

## ⚙️ Requisitos Previos (Prerequisites)
Para ejecutar este entorno de forma local, es necesario disponer de:
* **Docker y Docker Compose** (para levantar el clúster de Kafka).
* **Python 3.11+** (se recomienda el uso de entornos virtuales `venv` o `uv`).
* **Java Development Kit (JDK 11)** configurado en el `JAVA_HOME` (requisito indispensable para ejecutar Apache Spark).

---

## 🚀 Instrucciones de Despliegue y Ejecución

### 1. Clonación y Entorno Virtual
Clona este repositorio en tu máquina local e instala las dependencias necesarias:
```bash
git clone [https://github.com/Jorge-swim-1/tripadvisor-data-pipeline.git](https://github.com/Jorge-swim-1/tripadvisor-data-pipeline.git)
cd tripadvisor-data-pipeline
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
pip install apache-airflow polars pyarrow pyspark==3.5.0