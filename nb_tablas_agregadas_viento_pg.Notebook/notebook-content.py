# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b3812b33-8360-46f7-a885-bc1836264b85",
# META       "default_lakehouse_name": "lh_silver_pg",
# META       "default_lakehouse_workspace_id": "1867f4b3-ee9d-43a9-90ed-724dca17ead9",
# META       "known_lakehouses": [
# META         {
# META           "id": "b3812b33-8360-46f7-a885-bc1836264b85"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## **SPARK** - Tabla agregada de Viento por Año, Mes y Día

# CELL ********************

# 1) Leer desde el origen

df_original = spark.sql(
    """
    SELECT
    *
    FROM dbo.fact_estaciones
    """
)

display(df_original)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Realizar las agregaciones con Spark

from pyspark.sql import functions as F

# 1. Extraemos año, mes y día de la columna 'time'
# 2. Agrupamos por esos campos
# 3. Aplicamos las funciones max, min y avg con sus respectivos alias
df_agregado = df_original.withColumn("anio", F.year("time")) \
                         .withColumn("mes", F.month("time")) \
                         .withColumn("dia", F.dayofmonth("time")) \
                         .groupBy("anio", "mes", "dia") \
                         .agg(
                             F.max("dd_valor").alias("dir_racha1"),
                             F.min("dd_valor").alias("dir_racha2"),
                             F.avg("ff_valor").alias("vv_viento_promedio")
                         )

# Mostramos el resultado
display(df_agregado)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) Guardar tabla

df_agregado.write.format("delta").mode("overwrite").saveAsTable("dbo.fact_viento_ymd")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **SQL** - Tabla agregada de Viento por Año y Mes

# CELL ********************

# Creación directa en SQL Spark - Sólo una celda
df_inicio = spark.sql(
    """
    SELECT
        year( time ) AS anio,
        month( time ) AS mes,
        MAX( dd_valor ) AS dir_racha1, -- dirección expresada en grados desde el norte
        MIN( dd_valor ) AS dir_racha2, -- dirección expresada en grados desde el norte
        ROUND( AVG(ff_valor) , 2 ) AS vv_viento_promedio , -- Velocidad en nudos
        CodigoNacional
    FROM dbo.fact_estaciones
    GROUP BY 
        year( time ), 
        month( time ), 
        CodigoNacional
    """
)

df_inicio.write.format("delta").mode("overwrite").saveAsTable("dbo.fact_viento_ym")

df_review = spark.sql(
    """
    SELECT
    *
    FROM dbo.fact_viento_ym
    """
)

display( df_review)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
