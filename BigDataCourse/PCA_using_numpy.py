# Databricks notebook source exported at Thu, 14 Apr 2016 05:49:05 UTC
# MAGIC %md ### Performing PCA on vectors with NaNs
# MAGIC This notebook demonstrates the use of numpy arrays as the content of RDDs

# COMMAND ----------

import numpy as np

def outerProduct(X):
  """Computer outer product and indicate which locations in matrix are undefined"""
  O=np.outer(X,X)
  N=1-np.isnan(O)
  return (O,N)
def sumWithNan(M1,M2):
  """Add two pairs of matrix,count"""
  (X1,N1)=M1
  (X2,N2)=M2
  N=N1+N2
  X=np.nansum(np.dstack((X1,X2)),axis=2)
  return (X,N)
  

# COMMAND ----------

# computeCov recieves as input an RDD of np arrays, all of the same length, and computes the covariance matrix for that set of vectors
def computeCov(RDDin):
  RDD=RDDin.map(lambda v:np.insert(v,0,1)) # insert a 1 at the beginning of each vector so that the same 
                                           #calculation also yields the mean vector
  OuterRDD=RDD.map(outerProduct)   # separating the map and the reduce does not matter because of Spark uses lazy execution.
  (S,N)=OuterRDD.reduce(sumWithNan)
  # Unpack result and compute the covariance matrix
  #print 'RDD=',RDD.collect()
  print 'shape of S=',S.shape,'shape of N=',N.shape
  #print 'S=',S
  #print 'N=',N
  E=S[0,1:]
  NE=N[0,1:]
  print 'shape of E=',E.shape,'shape of NE=',NE.shape
  Mean=E/NE
  O=S[1:,1:]
  NO=N[1:,1:]
  Cov=O/NO - np.outer(Mean,Mean)
  return Cov,Mean

# COMMAND ----------

# MAGIC %md #### Demonstration on a small example

# COMMAND ----------

A=np.array([1,2,3,4,np.nan,5,6])
B=np.array([2,np.nan,1,1,1,1,1])
np.nansum(np.dstack((A,B)),axis=2)

# COMMAND ----------

RDD=sc.parallelize([A,B])

# COMMAND ----------

computeCov(RDD)

# COMMAND ----------

# MAGIC %md #### Demonstration on real data
# MAGIC The following cells demonstrate the use of the code we wrote on the maximal-dayly temperature records for the state of california.

# COMMAND ----------

TMAX = sqlContext.sql("select * from weather where measurement = 'TMAX'")

# COMMAND ----------

print type(TMAX)
print TMAX.count()

# COMMAND ----------

# MAGIC %run /Users/yfreund@ucsd.edu/Vault

# COMMAND ----------

AWS_BUCKET_NAME = "mas-dse-public" 
MOUNT_NAME = "NCDC-weather"
dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
output_code=dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
print 'Mount output status=',output_code
file_list=dbutils.fs.ls('/mnt/%s/Weather'%MOUNT_NAME)
file_list

# COMMAND ----------

US_Weather_parquet='/mnt/NCDC-weather/Weather/US_Weather.parquet/'
df = sqlContext.sql("SELECT * FROM  parquet.`%s`  where measurement=='TMAX'"%US_Weather_parquet)
df.show(5)

# COMMAND ----------

# We transform the dataframe into an RDD of numpy arrays
# remove the entries that do not correspond to temperature and devide by 10 so that the result is in centigrates.
rdd=df.map(lambda v:np.array(v[3:-4])/10).cache()
rdd.count()

# COMMAND ----------

rows=rdd.take(5)
len(rows[1])

# COMMAND ----------

import matplotlib.pyplot as plt
UnDef=rdd.map(lambda row:sum(np.isnan(row))).collect()
fig, ax = plt.subplots()
ax.hist(UnDef,bins=36)
display(fig)

# COMMAND ----------

# Remove entries that have 10 or more nan's
rdd=rdd.filter(lambda row:sum(np.isnan(row))<1)
rdd.count()

# COMMAND ----------

rdd.first()

# COMMAND ----------

# MAGIC %md ## compute covariance

# COMMAND ----------

OUT=computeCov(rdd)
OUT

# COMMAND ----------

[(i,OUT[i].shape) for i in range(len(OUT))]

# COMMAND ----------

(Cov,Mean)=OUT

# COMMAND ----------

Cov[:10,:10]

# COMMAND ----------

from numpy import linalg as LA
w,v=LA.eig(Cov)

# COMMAND ----------

from datetime import date
from numpy import shape
import matplotlib.pyplot as plt
import pylab as py
from pylab import ylabel,grid,title

dates=[date.fromordinal(i) for i in range(1,366)]
def YearlyPlots(T,ttl='',size=(10,7)):
    fig=plt.figure(1,figsize=size,dpi=300)
    fig, ax = plt.subplots(1)
    if shape(T)[0] != 365:
        raise ValueError("First dimension of T should be 365. Shape(T)="+str(shape(T)))
    ax.plot(dates,T);
    # rotate and align the tick labels so they look better
    fig.autofmt_xdate()
    ylabel('temperature')
    grid()
    title(ttl)
    return fig
fig=YearlyPlots(v[:,:4],'Eigen-Vectors')
display(fig)

# COMMAND ----------

fig=YearlyPlots(Mean,'Mean')
display(fig)

# COMMAND ----------

Var=np.cumsum(w)
Var=Var/Var[-1]
fig, ax = plt.subplots()
#ax.plot(x, Mean, 'r')
ax.plot(x,Var)
ax.grid()
display(fig)

# COMMAND ----------

