# Databricks notebook source exported at Mon, 7 Sep 2015 23:25:02 UTC
import urllib
AWS_BUCKET_NAME = "mas-dse-public" 
MOUNT_NAME = "NCDC-weather"
# Access Key of Yoav / DSE
ACCESS_KEY =  # ACCESS_KEY
SECRET_KEY =  # SECRET_KEY
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")


# COMMAND ----------

dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

file_list=dbutils.fs.ls('/mnt/NCDC-weather/WeatherUncompressed/')
[file.path for file in file_list[:3]]

# COMMAND ----------

data = sc.textFile("/mnt/%s/WeatherUncompressed/ALLaa" % MOUNT_NAME)
index = data.take(1)[0].split(',')
# The first three attributes in line 1 are in different order than those in rest of the lines,
# so we have to swap them into the correct order.
index[1], index[2] = index[2], index[1]
print index

# COMMAND ----------

data.take(3)

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS Weather

# COMMAND ----------

data_sample=data.sample(False,0.0001).cache()
data_sample.collect()
type(data),type(data_sample)

# COMMAND ----------

data_sample.count()

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

# MAGIC %run /Users/yfreund@ucsd.edu/loggly

# COMMAND ----------

dbutils.fs.ls('/user/hive/warehouse/')

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/weather/',recurse=True)

# COMMAND ----------

def extract(d):
  logger.info("message here")
  return d

# COMMAND ----------

from pyspark.sql import Row
import numpy as np
# 1. Split each line using comma,
# 2. remove first line (which is the names of each column)
# 3. sort the data based on the "year" attribute

dataRows=range(len(file_list))
dataFrames=range(len(file_list))

for i in range(len(file_list)):
    filename=file_list[i].path
    data = sc.textFile(filename)
    dataRows[i] = data.map(lambda s: s.split(',')) \
               .filter(lambda d: d[0] != 'station') \
               .map(lambda d: tuple(d[0:2]) + tuple([convert(x) for x in d[2:]])) \
               .sortBy(lambda d: d[2])
    dataFrames[i] = sqlContext.createDataFrame(dataRows[i], index)
    if i==0:
      combinedDataFrame=dataFrames[i]
    else:
      combinedDataFrame=combinedDataFrame.unionAll(dataFrames[i])
    print filename
    print combinedDataFrame.count()
#dataframe.saveAsTable("Weather")

# COMMAND ----------

combinedDataFrame.count()

# COMMAND ----------

combinedDataFrame.saveAsTable("Weather")

# COMMAND ----------

combinedDataFrame.write.parquet("/mnt/%s/Weather/parquet/Weather.parquet" % MOUNT_NAME)

# COMMAND ----------

import pandas as pd
import context
dataframe = context.table("Weather")

# COMMAND ----------

dbutils.fs.ls("/mnt/%s/Weather.parquet" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %sql select * from Weather limit 10

# COMMAND ----------

# MAGIC %sql  select W.measurement,COUNT(*) from Weather W group by W.measurement

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/NCDC-weather/Weather/Info')

# COMMAND ----------

stations_txt = sc.textFile('dbfs:/mnt/NCDC-weather/Weather/Info/ghcnd-stations_buffered.txt')

# COMMAND ----------

line=stations_txt.take(3)[2]
line

# COMMAND ----------

IV. FORMAT OF "ghcnd-stations.txt"

------------------------------
Variable   Columns   Type
------------------------------
ID            1-11   Character
LATITUDE     13-20   Real
LONGITUDE    22-30   Real
ELEVATION    32-37   Real
STATE        39-40   Character
NAME         42-71   Character
GSNFLAG      73-75   Character
HCNFLAG      77-79   Character
WMOID        81-85   Character
------------------------------


# COMMAND ----------

#0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
#          1         2         3         4         5         6         7         8         9
#AJ000037575  41.5500   46.6670  490.0    ZAKATALA                               37575
#US1CAAL0002  37.7075 -122.0687   87.5 CA CASTRO VALLEY 0.5 WSW
colspecs = [(0, 11), (11, 20), (20, 30), (30, 37),(37,41),(41,54),(72,76),(76,80),(80,86)]
colnames=['ID','latitude','longitude','elevation','state','name','GSNFLAG','HCNFLAG','WMOID']
coltypes=[type(x) for x in ['str',1.1,1.1,1.1,'str','str','str','str','str']]
print len(colspecs),len(colnames),len(coltypes)
coltypes

# COMMAND ----------

from string import strip
def parse_station(line):
  out=range(len(colspecs))
  for i in range(len(colspecs)):
    fr,to = colspecs[i]
    value = coltypes[i](line[fr:to])
    if type(value)==str:
      value=strip(value)
    #print colnames[i],'\t"%s"'%str(value)
    out[i]=value
  return tuple(out)

# COMMAND ----------

stations_text=sc.textFile("/mnt/%s/Weather/Info/ghcnd-stations_buffered.txt" % MOUNT_NAME)

# COMMAND ----------

stations_rows=stations_text.map(parse_station)

# COMMAND ----------

stations_rows.take(3)

# COMMAND ----------

station_info=sqlContext.createDataFrame(stations_rows, colnames)

# COMMAND ----------

stat

# COMMAND ----------

type(station_info.select('state'))

# COMMAND ----------

station_info.state.cast()

# COMMAND ----------

states=station_info.select('state')
states.distinct().count()


# COMMAND ----------

states[:20]

# COMMAND ----------

set([1,2,3,2,5,3])

# COMMAND ----------

station_info.saveAsTable('Stations')

# COMMAND ----------

len(stations_rows)

# COMMAND ----------

dataFrames[0].count()

# COMMAND ----------

type(station_info)

# COMMAND ----------

