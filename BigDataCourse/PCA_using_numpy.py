# Databricks notebook source exported at Mon, 29 Feb 2016 01:56:10 UTC
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
  RDD=RDDin.map(lambda v:np.insert(v,0,1)) # insert a 1 at the beginning of each vector so that the same 
                                           #calculation also yields the mean vector
  OuterRDD=RDD.map(outerProduct)   # separating the map and the reduce does not matter because of Spark uses lazy execution.
  (S,N)=OuterRDD.reduce(sumWithNan)
  # Unpack result and compute the covariance matrix
  #print 'RDD=',RDD.collect()
  print S.shape, N.shape
  #print 'S=',S
  #print 'N=',N
  E=S[0,1:]
  NE=N[0,1:]
  Mean=E/NE
  O=S[1:,1:]
  NO=N[1:,1:]
  Cov=O/NO - np.outer(Mean,Mean)
  return Cov,Mean
  


# COMMAND ----------

A=np.array([1,2,3,4,np.nan,5,6])
B=np.array([2,np.nan,1,1,1,1,1])
np.nansum(np.dstack((A,B)),axis=2)

# COMMAND ----------

RDD=sc.parallelize([A,B])

# COMMAND ----------

computeCov(RDD)

# COMMAND ----------

#Ca = sqlContext.sql("select * from weather2 where ((state='CA' or state='CO' or state='AZ' or state='OR' or state='NE') and measurement = 'TMAX')")
Ca = sqlContext.sql("select * from weather2 where (state='CA' and measurement = 'TMAX')")

# COMMAND ----------

states=Ca.map(lambda row:(row[-1],1)).collect()

# COMMAND ----------

from collections import Counter
C=Counter()
for s,i in states:
  C[s]+=1
C

# COMMAND ----------

Ca.first()

# COMMAND ----------

RDD_ca=Ca.map(lambda v:np.array(v[3:-1])/10)

# COMMAND ----------

RDD_ca=RDD_ca.filter(lambda row:sum(np.isnan(row))<10)

# COMMAND ----------

RDD_ca.first()

# COMMAND ----------

UnDef=RDD_ca.map(lambda row:sum(np.isnan(row))).collect()

# COMMAND ----------

x = range(365)
fig, ax = plt.subplots()
ax.hist(UnDef,bins=36)
display(fig)

# COMMAND ----------

OUT=computeCov(RDD_ca)
OUT

# COMMAND ----------

[(i,OUT[i].shape) for i in range(len(OUT))]

# COMMAND ----------

(Cov,Mean)=OUT

# COMMAND ----------

import pylab as py

# COMMAND ----------

Cov[:10,:10]

# COMMAND ----------

from numpy import linalg as LA
w,v=LA.eig(Cov)

# COMMAND ----------

from datetime import date
from numpy import shape
import matplotlib.pyplot as plt
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
fig=YearlyPlots(-v[:,0:3],'Temperature')
display(fig)

# COMMAND ----------

display(Mean)

# COMMAND ----------

fig =plt.figure(1,figsize=(10,7),dpi=300)
fig.axes.plot([0,1,0])

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = range(365)
print len(x)
fig, ax = plt.subplots()
ax.plot(x, Mean, 'r')
ax.plot(x,v[:,0]*1000)
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

print Var[:10]

# COMMAND ----------

v.shape

# COMMAND ----------

