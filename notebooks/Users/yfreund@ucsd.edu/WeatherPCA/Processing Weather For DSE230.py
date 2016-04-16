# Databricks notebook source exported at Sat, 16 Apr 2016 03:35:17 UTC
# MAGIC %run /Users/yfreund@ucsd.edu/Vault

# COMMAND ----------

AWS_BUCKET_NAME = "mas-dse-public" 
MOUNT_NAME = "NCDC-weather"
#print "ACCESS_KEY=",ACCESS_KEY
#print "SECRET_KEY=",SECRET_KEY
#print "ENCODED_SECRET_KEY=",ENCODED_SECRET_KEY
dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
output_code=dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
print output_code


# COMMAND ----------

# MAGIC %md ### This notebook demonstrates how to process the full NCDC weather dataset inside spark.

# COMMAND ----------

file_list=dbutils.fs.ls('/mnt/%s/Weather/'%MOUNT_NAME)
for file in file_list:
  print file

# COMMAND ----------

parquet_file='/mnt/NCDC-weather/Weather/US_Weather.parquet/' #|'/mnt/%s/Weather/parquet/Weather.parquet/'%MOUNT_NAME
df = sqlContext.read.load(parquet_file)
#df = sqlContext.sql("SELECT * FROM WHERE parquet.`%s`"%parquet_file)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md #### Read information about stations that are within the continental USA
# MAGIC (Plus some margins)
# MAGIC 
# MAGIC * **Northernmost point**
# MAGIC Northwest Angle, Minnesota (49°23'4.1" N) 
# MAGIC * **Southernmost point**
# MAGIC Ballast Key, Florida ( 24°31′15″ N) 
# MAGIC * **Easternmost point**
# MAGIC Sail Rock, just offshore of West Quoddy Head, Maine (66°57' W) ~ -66
# MAGIC * **Westernmost point**
# MAGIC Bodelteh Islands offshore from Cape Alava, Washington (124°46' W) ~ -124

# COMMAND ----------

stations_parquet='/mnt/NCDC-weather/Weather/Weather_Stations.parquet/'
stations_df = sqlContext.sql("SELECT * FROM  parquet.`%s`  where latitude>24 and latitude<50 and longitude > -125 and longitude < -66"%stations_parquet)
stations_df.show(5)

# COMMAND ----------

# select just ID logitude latitude and elevation
stations_long_lat=stations_df.select(['ID','longitude','latitude','elevation'])
stations_long_lat.columns

# COMMAND ----------

stations_long_lat.show(5)

# COMMAND ----------

df.columns[:5]

# COMMAND ----------

# Join the station table with the main dataframe. THis both removes stations far from the US 
# and adds columns corresponding to longitude, latitude, and elevation.

US_df=df.join(stations_long_lat, stations_long_lat.ID == df.station,'inner').drop('ID')

# COMMAND ----------

US_df=df
US_df.count()

# COMMAND ----------

US_df.columns[:5],US_df.columns[-5:]

# COMMAND ----------

count_rows=US_df.groupby('measurement').count().collect()

# COMMAND ----------

counts=[(e['measurement'],e['count']) for e in count_rows]
counts.sort(key=lambda x:x[1],reverse=True)
counts

# COMMAND ----------

L=US_df.take(10)

# COMMAND ----------

def delta(x):
  import numpy as np
  c=sum([np.isnan(x[str(i)]) for i in range(1,366)])
  z=np.zeros(365)
  z[364-c]=1.
  return (x['measurement'],z)
[delta(l) for l in L]

# COMMAND ----------



# COMMAND ----------

from pickle import dumps
dbutils.fs.put("/mnt/NCDC-weather/Weather/US_counts.pickle",dumps(US_counts),True)

# COMMAND ----------

#Create a sample RDD for debugging
Sample=US_df.sample(False,0.000001)
Sample.count()

# COMMAND ----------

US_counts_RDD=US_df.map(delta).reduceByKey(lambda x,y: x+y).cache()

# COMMAND ----------

US_counts=US_counts_RDD.collect()

# COMMAND ----------

#Sort according to total count.
US_counts3=[(e[0],sum(e[1]),e[1]) for e in US_counts]
US_counts3.sort(key=lambda x:x[1],reverse=True)
US_counts3[1]

# COMMAND ----------

import matplotlib.pyplot as plt
columns=5; rows=5
fig,ax = plt.subplots(rows,columns)
for i in range(columns*rows):
  name,total,counts = US_counts3[i]
  if i>=rows*columns: break
  col = i % columns
  row = i / columns
  axis=ax[row][col]
  axis.step(range(1,len(counts)+1),counts)
  #axis.title=name+':  '
  #axis.xlim([0,365])
display(fig)

# COMMAND ----------

from pickle import dumps
dbutils.fs.put("/mnt/NCDC-weather/Weather/US_counts.pickle",dumps(US_counts),True)



# COMMAND ----------

# MAGIC %md ### END