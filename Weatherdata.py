# Databricks notebook source exported at Thu, 3 Sep 2015 22:31:16 UTC
import urllib
AWS_BUCKET_NAME = "cse-jalafate" #weather.raw_data"
MOUNT_NAME = "NCDC-weather"
# Access Key of Julaiti
ACCESS_KEY = "AKIAJBNVEFYWZSWIF2HQ" # ACCESS_KEY
SECRET_KEY = "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" # SECRET_KEY
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")


# COMMAND ----------

dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

data = sc.textFile("/mnt/%s/ALL.csv_2" % MOUNT_NAME)
index = data.take(1)[0].split(',')
# The first three attributes in line 1 are in different order than those in rest of the lines,
# so we have to swap them into the correct order.
index[1], index[2] = index[2], index[1]
print index

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS Weather

# COMMAND ----------

data_sample=data.sample(False,0.0001).cache()
data_sample.collect()
type(data),type(data_sample)

# COMMAND ----------

data_sample.count()
type(data_sample)

# COMMAND ----------

def convert(x):
  x = x.strip()
  if x == '':
    # The value goes missing
    return np.nan
  return float(x)

# COMMAND ----------

# MAGIC %sql DROP TABLE  Weather

# COMMAND ----------

from pyspark.sql import Row
# 1. Split each line using comma,
# 2. remove first line (which is the names of each column)
# 3. sort the data based on the "year" attribute
dataRows = data.map(lambda s: s.split(',')) \
               .filter(lambda d: d[0] != 'station') \
               .map(lambda d: tuple(d[0:2]) + tuple([convert(x) for x in d[2:]])) \
               .sortBy(lambda d: d[2])
dataframe = sqlContext.createDataFrame(dataRows, index)
dataframe.saveAsTable("Weather")

# COMMAND ----------

dataframe.saveAsTable("Weather3")

# COMMAND ----------

dataframe.write.parquet(""/mnt/%s/Weather.parquet" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %sql select * from Weather3 LIMIT 20

# COMMAND ----------

data.top(5)

# COMMAND ----------

# MAGIC %sql select * from Weather limit 10

# COMMAND ----------

# MAGIC %sql  select W.measurement,COUNT(*) from Weather W group by W.measurement

# COMMAND ----------

