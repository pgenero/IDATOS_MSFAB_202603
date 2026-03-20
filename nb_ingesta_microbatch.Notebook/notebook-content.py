# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a34477d-cffc-4f92-86a9-b539e39521f7",
# META       "default_lakehouse_name": "lh_bronze_pg",
# META       "default_lakehouse_workspace_id": "1867f4b3-ee9d-43a9-90ed-724dca17ead9",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a34477d-cffc-4f92-86a9-b539e39521f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Importar librerías
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F


# Definir Rutas
ruta_origen = "abfss://WS2603_PABLO@onelake.dfs.fabric.microsoft.com/lh_bronze_pg.Lakehouse/Files/sensor_simulado"
ruta_chequeo = "abfss://WS2603_PABLO@onelake.dfs.fabric.microsoft.com/lh_bronze_pg.Lakehouse/Files/mdg_chequeo"
nombre_tabla = "dbo.sensores_simulados"

esquema_archivo = StructType()\
    .add("id",        StringType())\
    .add("temp",      DoubleType())\
    .add("humedad",   DoubleType())\
    .add("viento",    DoubleType())\
    .add("timestamp", TimestampType())

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {nombre_tabla}(
        id STRING,
        temperatura DOUBLE,
        humedad DOUBLE,
        viento DOUBLE,
        ts_registro TIMESTAMP,
        ts_procesamiento TIMESTAMP
    ) USING DELTA
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DELTA STREAM o MICROBATCHING

archivo_raiz = spark.readStream\
    .schema(esquema_archivo)\
    .option("maxFilesPerTrigger", 1)\
    .json(ruta_origen)

# Agregar al DataFrame el campo "ts_process"
archivo_df = archivo_raiz.withColumn("ts_process", F.current_timestamp() )

deltastream = archivo_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_chequeo)\
    .toTable(nombre_tabla)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Monitoreo del Streaming
print(deltastream.status)
print(deltastream.lastProgress)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Comando para detener el streaming
deltastream.stop

# Para reiniciar el stream correr de nuevo 'DELTA STREAM o MICROBATCHING'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
