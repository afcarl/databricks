# Databricks notebook source exported at Mon, 15 Feb 2016 07:48:35 UTC
import numpy as np

# COMMAND ----------

def outerProduct(X):
  O=np.outer(X,X)
  N=1-np.isnan(O)
  return (O,N)


# COMMAND ----------

def sumWithNan(M1,M2):
  (X1,N1)=M1
  (X2,N2)=M2
  N=N1+N2
  X=np.nansum(np.dstack((X1,X2)),axis=2)
  return (X,N)
  

# COMMAND ----------

# computeCov recieves as input an RDD of np arrays, all of the same length, and computes the covariance matrix for that set of vectors
def computeCov(RDDin):
  RDD=RDDin.map(lambda v:np.insert(v,1,1)) # insert a 1 at the beginning of each vector so that the same 
                                           #calculation also yields the mean vector
  OuterRDD=RDD.map(outerProduct)
  (S,N)=OuterRDD.reduce(sumWithNan)
  E=S[0,1:]
  NE=N[0,1:]
  Mean=E/NE
  O=S[1:,1:]
  NO=N[1:,1:]
  Cov=O/NO - np.outer(Mean,Mean)
  return np.outer(Mean,Mean),O,N,O/NO,Cov
  
computeCov([A,B])


# COMMAND ----------

E=S[0][0,1:]

# COMMAND ----------

A=np.array([1,2,3,4,np.nan,5,6])
B=np.array([1,np.nan,1,1,1,1,1])
np.nansum(np.dstack((A,B)),axis=2)

# COMMAND ----------

np.insert(A,0,1)

# COMMAND ----------

outerProduct(B)

# COMMAND ----------

(X,N)=sumWithNan(outerProduct(A),outerProduct(B))

# COMMAND ----------

X/N

# COMMAND ----------

np.nansum(np.dstack((A,B)),axis=2)

# COMMAND ----------

np

# COMMAND ----------

RDD=sc.parallelize([A,B])

# COMMAND ----------

OuterRDD=RDD.map(outerProduct)

# COMMAND ----------

C=OuterRDD.reduce(sumWithNan)
C

# COMMAND ----------

Ca = sqlContext.sql("select * from weather2 where (state='CA' and measurement = 'TMAX')")

# COMMAND ----------

Ca.count()

# COMMAND ----------

RDD_ca=Ca.map(lambda v:np.array(v[3:-1])/10)

# COMMAND ----------

RDD_ca.first()

# COMMAND ----------

OUT=computeCov(RDD_ca)
OUT

# COMMAND ----------

