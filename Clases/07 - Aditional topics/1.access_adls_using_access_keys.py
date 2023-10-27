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

# COMMAND ----------

storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""
blobContainerName = "raw"
mountPoint = "/mnt/sarsv2023/raw"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)
