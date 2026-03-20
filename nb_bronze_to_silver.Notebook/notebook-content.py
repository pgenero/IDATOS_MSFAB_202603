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
# META         },
# META         {
# META           "id": "b3812b33-8360-46f7-a885-bc1836264b85"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_bronze = spark.read.table("lh_bronze_pg.dbo.viento_dmc")
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ft_viento = spark.sql(
    """
    SELECT
        time, 
        dd_valor, 
        ff_valor, 
        VRB_Valor, 
        CodigoNacional
    FROM lh_bronze_pg.dbo.viento_dmc
    WHERE 
        dd_Valor IS NOT NULL 
        OR ff_Valor IS NOT NULL 
        OR VRB_Valor IS NOT NULL
    """)

df_dim_estaciones = spark.sql(
    """
    SELECT
        DISTINCT( CodigoNacional ),
        latitud,
        longitud,
        nombreEstacion
    FROM lh_bronze_pg.dbo.viento_dmc
    """)

display(df_ft_viento)
display(df_dim_estaciones)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_estaciones.write.format("delta").mode("overwrite").saveAsTable("lh_silver_pg.dbo.dim_estaciones")
df_ft_viento.write.format("delta").mode("overwrite").saveAsTable("lh_silver_pg.dbo.fact_estaciones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
