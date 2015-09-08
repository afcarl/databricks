# Databricks notebook source exported at Tue, 8 Sep 2015 22:31:43 UTC

# MAGIC %run /Users/yfreund@ucsd.edu/Vault

# COMMAND ----------

# MAGIC %md ### This notebook demonstrates how to process the full NCDC weather dataset inside spark.

# COMMAND ----------

dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

file_list=dbutils.fs.ls('/mnt/NCDC-weather/WeatherUncompressed/')
[file.path for file in file_list[:3]]

# COMMAND ----------
