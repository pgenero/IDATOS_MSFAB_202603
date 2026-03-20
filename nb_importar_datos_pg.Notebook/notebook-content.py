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

import requests

BASE_URL   = "https://thesimpsonsapi.com/api/characters"
START_PAGE = 60
TIMEOUT    = 30

all_records = []
page        = START_PAGE

while True:
    url = f"{BASE_URL}?page={page}"

    try:
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Pagina {page}: {e}")
        break

    characters = data.get("results", [])

    if not characters:
        print(f"[INFO] Sin registros en pagina {page}. Fin.")
        break

    all_records.extend(characters)
    print(f"[OK] Pagina {page} — {len(characters)} personajes | Total acumulado: {len(all_records)}")

    next_url = data.get("next")
    if not next_url:
        print("[INFO] Ultima pagina alcanzada.")
        break

    page += 1

print(f"\nTotal registros extraidos: {len(all_records)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

# Schema explicito basado en la estructura real del response
schema = StructType([
    StructField("id",            IntegerType(), nullable=True),
    StructField("age",           IntegerType(), nullable=True),
    StructField("birthdate",     StringType(),  nullable=True),
    StructField("gender",        StringType(),  nullable=True),
    StructField("name",          StringType(),  nullable=True),
    StructField("occupation",    StringType(),  nullable=True),
    StructField("portrait_path", StringType(),  nullable=True),
    StructField("phrases",       ArrayType(StringType()), nullable=True),
    StructField("status",        StringType(),  nullable=True),
])

# Crear DataFrame con schema explicito
df_characters = spark.createDataFrame(all_records, schema=schema)

# Exploracion basica
print(f"Registros : {df_characters.count()}")
print(f"Columnas  : {df_characters.columns}")
df_characters.printSchema()
df_characters.show(10, truncate=False)

# Opcional: guardar como tabla Delta en Lakehouse
# df_characters.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("bronze.simpsons_characters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_characters.write.format("delta").mode("overwrite").saveAsTable("lh_bronze_pg.dbo.simpsons_personajes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
