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
# META         },
# META         {
# META           "id": "ad96c1f7-25b3-4830-bb8b-f7857db2b465"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Reparar DF para Entrenar

df_para_entrenar = spark.read.table("dbo.fact_estaciones")
df_para_entrenar.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Agregación diaria de viento promedio por estación
from pyspark.sql import functions as F

df_agregado = (
    df_para_entrenar
    .withColumn("fecha", F.to_date("time"))
    .withColumn("anio",  F.year("time"))
    .withColumn("mes",   F.month("time"))
    .withColumn("dia",   F.dayofmonth("time"))
    .groupBy("CodigoNacional", "anio", "mes", "dia", "fecha")
    .agg(
        F.round(F.avg("ff_valor"), 2).alias("vviento_promedio")
    )
    .orderBy("CodigoNacional", "anio", "mes", "dia")
)

df_agregado.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardar en Gold

df_agregado.write.format("delta").mode("overwrite")\
.saveAsTable("lh_gold_pg.dbo.ml_train_data_viento_promedio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
