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

# ## Dimensión Empresa
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
    "idempresa",
    "empresa",
)

# 3) Limpiar datos con trim y pasar texto a mayúsculas
df_limpio = df_transform.withColumn("empresa", f.trim(f.col("empresa"))) \
                        .withColumn("idempresa", f.trim(f.col("idempresa")))
df_limpio = df_limpio.withColumn("empresa", f.upper(f.col("empresa"))) \
                        .withColumn("idempresa", f.upper(f.col("idempresa")))

# 4) Remover duplicados en las columnas seleccionadas luego de la limpieza
df_valores = df_limpio.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Obtener mapeo de nombres de empresas
# Los nombres de las empresas en el origen se mapean ara identificarlas con el nombre de la empresa o grupo al que pertenecen sin importar la razón social

# CELL ********************

# 1) Leer el CSV de mapeo desde Bronze (Files)
path_mapeo = "abfss://WS2603_PABLO@onelake.dfs.fabric.microsoft.com/lh_bronze_oil_gas.Lakehouse/Files/mapeos/mapeo_nombre_empresas.csv"

df_mapeo = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .load(path_mapeo)

# 2) Unir (Join) con el mapeo para traer el 'nombre_empresa' limpio
# Usar 'left' por si alguna empresa no está en el CSV para no perderla
dim_empresa_join = df_valores.join(df_mapeo, on="idempresa", how="left")

# 3) Seleccionar las columnas a mantener de cada df post join
dim_empresa = dim_empresa_join.select(
    df_valores["idempresa"],
    df_valores["empresa"],
    df_mapeo["empresa_actual"],
    df_mapeo["nombre"]
)

# 5) Guardar el resultado final en Silver
dim_empresa.write.mode("overwrite").saveAsTable("dim_empresa")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
