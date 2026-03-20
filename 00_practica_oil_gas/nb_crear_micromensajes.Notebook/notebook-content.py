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

# MARKDOWN ********************

# ## Simular Real Time
# El propósito del notebook es crear mensajes en formato JSON simulando un dispositivo IoT

# CELL ********************

# Importar las librerías

import os
import json
import uuid
import random
import time
# From + Import → Importar solo algunos elementos de la librería para eviar sobrecarga de trabajos (solo lo que uso)
from datetime import datetime
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType 
import pyspark.sql.functions as F

# Configuración
# Se crearán datos y aquí se plasma la configuración

# Ruta donde guardaremos los mensajes simulados
ruta_destino = "/lakehouse/default/Files/sensor_simulado"
num_mensajes = 100
seg_espera = 3

# Verificación
# Verificar si la ruta existe: SI el notebook continúa, NO el notebook para
os.makedirs(ruta_destino, exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_mensajes):

    # Variables generales
    # Estas variables se usan para construir el mensaje
    ahora = datetime.utcnow()
    ts_str = ahora.strftime("%Y%m%dT%H%M%S")

    # Estructura del mensaje
    registro = {
        "id": str(uuid.uuid4()) ,
        "temp": round(random.uniform(-2.0,38.0),2) ,
        "humedad": round(random.uniform(0.0,1.0),2) ,
        "viento": round(random.uniform(0.0,10.0),2),
        "timestamp": ahora.isoformat()
    }

    # Construir arcivo JSON
    filename = f"sensor_{ts_str}.json"
    filepath = os.path.join(ruta_destino, filename)

    # Materializar el fichero
    with open(filepath,"w") as f:
        json.dump(registro, f)
    
    # Mensaje de Ok
    print(f"OK - [{i+1} / {num_mensajes}] generado : {filename}")
    time.sleep(seg_espera)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
