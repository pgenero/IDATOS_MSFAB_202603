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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Pronóstico de Producción
# Transformaciones para obtener la *fact table* con los pronósticos de producción

# CELL ********************

# 1) Leer tabla stg_prod_oil_gas en Silver
df_stg_prod_oil_gas = spark.read.table("lh_silver_oil_gas.dbo.stg_prod_oil_gas")

# 2) Seleccionar las columnas necesarias
df_fact_pronostico_produccion = spark.sql(
    """
    WITH
    datos_pronostico AS (
        SELECT
            anio AS anio_reportado, -- para cambiar el pronostico a una fecha de reporte diferente (SCD)
            UPPER( TRIM( idempresa ) ) AS id_empresa, -- relación con dim_empresa
            UPPER( TRIM( idprovincia ) ) AS id_provincia, -- relación con dim_provincia
            CASE
                WHEN hidrocarburo = 'petroleo' THEN 1
                WHEN hidrocarburo = 'gas' THEN 2
                ELSE NULL
            END AS id_hidrocarburo,
            CASE
                WHEN CAST( idconcepto AS INT ) > 12 THEN CAST( CONCAT (anio_pronostico , '-01-01') AS DATE) -- para reportes anuales
                ELSE CAST( CONCAT (anio_pronostico, '-' , idconcepto, '-01') AS DATE) -- para reportes mensuales
            END AS fecha_pronostico, -- relación con dim_calendario: si es ANUAL fija el primer día del año pronosticado, si es MES fija el primero del mes
            CASE
                WHEN UPPER( tipoinforme ) = 'M' THEN 'MES'
                WHEN UPPER( tipoinforme ) = 'A' THEN 'ANUAL'
                ELSE NULL
            END AS tipo_informe, -- ANUAL devuelve valores agregados por Año   
            cantidad
        FROM lh_silver_oil_gas.dbo.stg_prod_oil_gas
        WHERE UPPER( idempresa ) <> 'SOU' -- Esta empresa es ficticia en el dataset
        )

-- agrupar datos y sumar cantidad: la granularidad puede estar en varias filas por el 'area' (removido) y por la clasificación en Gas
    SELECT
        anio_reportado,
        id_empresa,
        id_provincia,
        id_hidrocarburo,
        fecha_pronostico,
        tipo_informe,
        SUM( CAST( cantidad AS FLOAT ) ) AS cantidad
    FROM datos_pronostico
    GROUP BY anio_reportado, id_empresa, id_provincia, fecha_pronostico, id_hidrocarburo, tipo_informe

    """
)

df_fact_pronostico_produccion.write.format("delta").mode("overwrite").saveAsTable("lh_silver_oil_gas.dbo.fact_pronostico_produccion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
