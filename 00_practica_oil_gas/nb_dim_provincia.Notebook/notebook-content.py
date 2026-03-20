# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2535d86b-94b0-4146-b114-64905d6114f2",
# META       "default_lakehouse_name": "lh_silver_oil_gas",
# META       "default_lakehouse_workspace_id": "1867f4b3-ee9d-43a9-90ed-724dca17ead9",
# META       "known_lakehouses": [
# META         {
# META           "id": "2535d86b-94b0-4146-b114-64905d6114f2"
# META         },
# META         {
# META           "id": "40119c0e-03a7-4284-b32b-055c9028b952"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Dimensión Provincia
# Leer tabla stg_prod_oil_gas del Lakehose en Silver, realizar transformaciones y guardar tabla de dimensiones

# MARKDOWN ********************

# ### Importar librerías

# CELL ********************

from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transformar datos

# CELL ********************

# 1) Leer tabla tg_prod_oil_gas en Silver
df_stg_prod_oil_gas = spark.read.table("lh_silver_oil_gas.dbo.stg_prod_oil_gas")

# 2) Seleccionar las columnas necesarias
df_transform = df_stg_prod_oil_gas.select(
    "idprovincia",
    "provincia",
)

# 3) Limpiar datos con trim y pasar texto a mayúsculas
df_limpio = df_transform.withColumn("provincia", f.trim(f.col("provincia"))) \
                        .withColumn("idprovincia", f.trim(f.col("idprovincia")))
df_limpio = df_limpio.withColumn("provincia", f.upper(f.col("provincia"))) \
                        .withColumn("idprovincia", f.upper(f.col("idprovincia")))

# 4) Remover duplicados en las columnas seleccionadas luego de la limpieza
df_valores = df_limpio.dropDuplicates()

# 5) Guardar el resultado final en Silver
dim_provincia = df_valores
dim_provincia.write.format("delta").mode("overwrite").saveAsTable("lh_silver_oil_gas.dbo.dim_provincia")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
