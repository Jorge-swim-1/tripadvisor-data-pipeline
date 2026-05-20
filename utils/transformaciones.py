import polars as pl

def separar_dlq(df: pl.DataFrame, critical_cols: list) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Patrón DLQ (Dead Letter Queue): En lugar de borrar nulos, los auditamos.
    Devuelve dos DataFrames: (Datos Buenos, Datos Corruptos)
    """
    # Creamos una máscara: True si ALGUNA de las columnas críticas es nula
    condicion_nulos = pl.any_horizontal([pl.col(c).is_null() for c in critical_cols])
    
    # Filtramos
    df_corrupto = df.filter(condicion_nulos)
    df_bueno = df.filter(~condicion_nulos) # El símbolo ~ significa "lo contrario"
    
    return df_bueno, df_corrupto

def aplicar_window_functions(df: pl.DataFrame) -> pl.DataFrame:
    """
    Funciones de Ventana: Calcula métricas competitivas por ciudad.
    Esto demuestra un uso avanzado de Polars imposible de hacer rápido en Pandas.
    """
    # 1. Calculamos la nota media de todos los restaurantes de la misma ciudad
    df = df.with_columns(
        pl.col("avg_rating").mean().over("city").alias("city_avg_rating")
    )
    
    # 2. Calculamos la diferencia: ¿Este restaurante es mejor o peor que la media de su ciudad?
    df = df.with_columns(
        (pl.col("avg_rating") - pl.col("city_avg_rating")).alias("rating_diff_city")
    )
    return df

def feature_engineering_avanzado(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """Aplica las transformaciones numéricas y binarias."""
    df = df.with_columns(pl.col(config['numeric_columns']).fill_null(0))
    
    df = df.with_columns(
        pl.when(pl.col("price_level").str.strip_chars() == "$").then(1)
        .when(pl.col("price_level").str.strip_chars() == "$$ - $$$").then(2)
        .when(pl.col("price_level").str.strip_chars() == "$$$$").then(3)
        .otherwise(0).alias("price_level_num")
    )
    
    df = df.with_columns([
        pl.when(pl.col("claimed") == "Claimed").then(1).otherwise(0).alias("is_claimed"),
        pl.when(pl.col("vegetarian_friendly") == "Y").then(1).otherwise(0).alias("is_veg_friendly"),
        pl.when(pl.col("gluten_free") == "Y").then(1).otherwise(0).alias("is_gluten_free")
    ])
    
    return df