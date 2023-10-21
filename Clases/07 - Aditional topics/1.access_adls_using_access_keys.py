# Databricks notebook source
#conexion al Storage Account
spark.conf.set(
    "fs.azure.account.key.sarsv2023.dfs.core.windows.net",
    "4laHHRo3eLqlGttXboMIODFDxL85I+F8AitgF5cPS2HB+2CfzW8Wh8PKzscgVan/CFG8/i5unlWk+AStABkbNA=="
)

# COMMAND ----------

#conexión al container
dbutils.fs.ls("abfss://raw@sarsv2023.dfs.core.windows.net")

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@sarsv2023.dfs.core.windows.net/circuits.csv")
display(circuits_df)
