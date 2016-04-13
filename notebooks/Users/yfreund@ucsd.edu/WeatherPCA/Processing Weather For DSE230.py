# Databricks notebook source exported at Wed, 13 Apr 2016 22:01:30 UTC
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
#[file.path for file in file_list[:3]]

# COMMAND ----------

parquet_file='/mnt/NCDC-weather/Weather/Weather.parquet/' #|'/mnt/%s/Weather/parquet/Weather.parquet/'%MOUNT_NAME
df = sqlContext.read.load(parquet_file)
#df = sqlContext.sql("SELECT * FROM WHERE parquet.`%s`"%parquet_file)

# COMMAND ----------

df.count()

# COMMAND ----------

stations_parquet='/mnt/NCDC-weather/Weather/Weather_Stations.parquet/'
stations_df = sqlContext.sql("SELECT * FROM  parquet.`%s`  where latitude>24 and latitude<50 and longitude > -125 and longitude < -66"%stations_parquet)
stations_df.show(5)

# COMMAND ----------

stations_long_lat=stations_df.select(['ID','longitude','latitude','elevation'])
stations_long_lat.columns

# COMMAND ----------

stations_long_lat.show(5)

# COMMAND ----------

df.columns[:5]

# COMMAND ----------

US_df=df.join(stations_long_lat, stations_long_lat.ID == df.station,'inner')

# COMMAND ----------

US_df.count()

# COMMAND ----------

US_df.write.parquet("/mnt/NCDC-weather/Weather/US_Weather.parquet")

# COMMAND ----------

sqlContext.tableNames()

# COMMAND ----------

stations_df = sqlContext.table('stations')
stations_df.count()

# COMMAND ----------

stations_parquet="/mnt/NCDC-weather/Weather_Stations.parquet"
stations_df.write.parquet(stations_parquet)

# COMMAND ----------

# MAGIC %md #### Read all stations that are within the continental USA
# MAGIC Plus some margins:
# MAGIC * **Longitude:** 66 - 125
# MAGIC * **Latitude:** 24 - 50

# COMMAND ----------



# COMMAND ----------

'/mnt/NCDC-weather/Weather.parquet/'

# COMMAND ----------

# MAGIC %md #### Count stations
# MAGIC The following sql command counts the number of stations in each state in the US.

# COMMAND ----------

# MAGIC %sql select state as state,count(*) as count from stations where substr(id,0,2)=="US"  group by state

# COMMAND ----------

states = sqlContext.sql('select state as state,count(*) as count from stations where substr(id,0,2)=="US"  group by state')
states.sort(['state']).show(100)

# COMMAND ----------

dataframe.count()

# COMMAND ----------

Continental_states = """Alabama		AL
Arizona		AZ
Arkansas	AR
California	CA
Colorado	CO
Connecticut	CT
Delaware	DE
Florida		FL
Georgia		GA
Idaho		ID
Illinois	IL
Indiana		IN
Iowa		IA
Kansas		KS
Kentucky	KY
Louisiana	LA
Maine		ME
Maryland	MD
Massachusetts	MA
Michigan	MI
Minnesota	MN
Mississippi	MS
Missouri	MO
Montana		MT
Nebraska	NE
Nevada		NV
New Hampshire	NH
New Jersey	NJ
New Mexico	NM
New York	NY
North Carolina	NC
North Dakota	ND
Ohio		OH
Oklahoma	OK
Oregon		OR
Pennsylvania	PA
Rhode Island	RI
South Carolina	SC
South Dakota	SD
Tennessee	TN
Texas		TX
Utah		UT
Vermont		VT
Virginia	VA
Washington	WA
West Virginia	WV
Wisconsin	WI
Wyoming		WY""";

non_continental_states= """Alaska		AK
Hawaii		HI
"""

# COMMAND ----------

us_states={line.split('\t')[-1]:line.split('\t')[0] for line in Continental_states.split('\n')}
us_states


# COMMAND ----------

stations= sqlContext.sql('select * from stations')

# COMMAND ----------

stations.show()

# COMMAND ----------

cont_acronyms=us_states.keys()
print len(cont_acronyms)
print ','.join(cont_acronyms)

# COMMAND ----------

in_continental = [row.state in cont_acronyms for row in stations.select('state').collect()]
print len(in_continental)
print sum(in_continental)

# COMMAND ----------

# MAGIC %sql select s.ID as station from stations as s where s.state='CA'

# COMMAND ----------

# MAGIC %sql create table selectedstations as select s.ID from stations as s where s.state='CA'

# COMMAND ----------

# MAGIC %sql select * from selectedstations limit 10

# COMMAND ----------

# MAGIC %sql create table weather2 as select w.*, s.state from weather w join stations s on w.station = s.ID

# COMMAND ----------

# MAGIC %sql select * from weather2 where (state='CA' and measurement='TMAX') limit 10

# COMMAND ----------

# MAGIC %md subqueries are not supported in spark-sql:
# MAGIC `%sql select * from weather as w where (w.measurement = 'TMAX' and 
# MAGIC                                   w.station in (select s.ID as station from stations as s where s.state='CA')
# MAGIC                                  ) limit 10`

# COMMAND ----------

Ca = sqlContext.sql("select * from weather2 where (state='CA' and measurement = 'TMAX')")

# COMMAND ----------

Ca.count()

# COMMAND ----------

from pyspark.mllib.stat import Statistics

# COMMAND ----------

Row=Ca.first()
Row

# COMMAND ----------

import numpy as np
v=np.array(Row[3:-1])
sum(np.isnan(v))

# COMMAND ----------

TMAX=Ca.select([str(i) for i in range(1,365)])

# COMMAND ----------

from pyspark.mllib.stat import Statistics
#from pyspark.mllib.linalg.distributed import RowMatrix

mat = TMAX.rdd()

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print(summary.mean())
print(summary.variance())
print(summary.numNonzeros())

# COMMAND ----------

from pyspark.mllib.linalg import Vectors
rdd = sc.parallelize([Vectors.dense([2, 0, 0, -2]),
                      Vectors.dense([4, 5, 0,  3]),
                      Vectors.dense([6, 7, 0,  8])])

# COMMAND ----------

pyspark.mllib.linalg.help() 

# COMMAND ----------



# COMMAND ----------

pyspark.mllib.ver

# COMMAND ----------

TMAX.rdd()

# COMMAND ----------

1+1

# COMMAND ----------

