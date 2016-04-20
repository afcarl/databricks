# Databricks notebook source exported at Wed, 20 Apr 2016 06:07:38 UTC
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
  """Add two pairs of (matrix,count)"""
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
  NE=np.float64(N[0,1:])
  print 'shape of E=',E.shape,'shape of NE=',NE.shape
  Mean=E/NE
  O=S[1:,1:]
  NO=np.float64(N[1:,1:])
  Cov=O/NO - np.outer(Mean,Mean)
  # Output also the diagnal which is the variance for each day
  Var=np.array([Cov[i,i] for i in range(Cov.shape[0])])
  return E,NE,O,NO,Cov,Mean,Var

# COMMAND ----------

# Compute the overall distribution of values and the distribution of the number of nan per year
def find_percentiles(SortedVals,percentile):
  L=len(SortedVals)/percentile
  return SortedVals[L],SortedVals[-L]
  
def computeOverAllDist(rdd0):
  UnDef=rdd0.map(lambda row:sum(np.isnan(row))).collect()
  flat=rdd0.flatMap(lambda v:list(v)).filter(lambda x: not np.isnan(x)).cache()
  count,S1,S2=flat.map(lambda x: np.float64([1,x,x**2]))\
                  .reduce(lambda x,y: x+y)
  mean=S1/count
  std=np.sqrt(S2/count-mean**2)
  Vals=flat.sample(False,0.001).collect()
  SortedVals=sorted(Vals)
  low100,high100=find_percentiles(SortedVals,100)
  low1000,high1000=find_percentiles(SortedVals,1000)
  return {'UnDef':UnDef,\
          'mean':mean,\
          'std':std,\
          'SortedVals':SortedVals,\
          'low100':low100,\
          'high100':high100,\
          'low1000':low100,\
          'high1000':high1000
          }



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

N=sc.defaultParallelism
print 'Number of executors=',N

# COMMAND ----------

# MAGIC %run /Users/yfreund@ucsd.edu/Vault

# COMMAND ----------

AWS_BUCKET_NAME = "mas-dse-public" 
MOUNT_NAME = "NCDC-weather"
OPEN_BUCKET_NAME = "mas-dse-open"
OPEN_MOUNT_NAME = "OPEN-weather"
dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME)
dbutils.fs.unmount("/mnt/%s" % OPEN_MOUNT_NAME)
output_code=dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
print 'Mount public status=',output_code
output_code=dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, OPEN_BUCKET_NAME), "/mnt/%s" % OPEN_MOUNT_NAME)
print 'Mount open status=',output_code

file_list=dbutils.fs.ls('/mnt/%s/Weather'%MOUNT_NAME)
file_list

# COMMAND ----------

file_list=dbutils.fs.ls('/mnt/%s/Weather'%OPEN_MOUNT_NAME)
file_list

# COMMAND ----------

import numpy as np
US_Weather_parquet='/mnt/NCDC-weather/Weather/US_Weather.parquet/'
measurements=['TMAX','TMIN','TOBS','SNOW','SNWD','PRCP']
Query="SELECT * FROM parquet.`%s`\n\tWHERE "%US_Weather_parquet+"\n\tor ".join(["measurement='%s'"%m for m in measurements])
print Query
df = sqlContext.sql(Query)

rdd0=df.map(lambda row:(row['station'],((row['measurement'],row['year']),np.array([np.float64(row[str(i)]) for i in range(1,366)])))).cache()
#.sample(False,0.1).cache()
#print 'size of RDD=',rdd0.count()


# COMMAND ----------

meas='PRCP'
Query="SELECT * FROM parquet.`%s`\n\tWHERE measurement = '%s'"%(US_Weather_parquet,meas)
print Query
df = sqlContext.sql(Query)
rdd0=df.map(lambda row:(row['station'],((row['measurement'],row['year']),np.array([np.float64(row[str(i)]) for i in range(1,366)])))).cache()

rdd1=rdd0.sample(False,0.01)\
         .map(lambda (key,val): val[1])\
         .cache()\
         .repartition(N)
print rdd1.count()

STAT[meas]=computeOverAllDist(rdd1)   # Compute the statistics 
print meas,STAT.keys()
low1000 = STAT[meas]['low1000']  # unpack the statistics
high1000 = STAT[meas]['high1000']



# COMMAND ----------

rdd2=rdd1.map(lambda V: np.array([x if (x>low1000-1) and (x<high1000+1) else np.nan for x in V]))
#print meas,'before removing rows',rdd2.count()
# Remove entries that have 50 or more nan's
rdd3=rdd2.filter(lambda row:sum(np.isnan(row))<50)
#print meas,'after removing rows with too many nans',rdd3.count()
Clean_Tables[meas]=rdd3.cache().repartition(N)
C=Clean_Tables[meas].count()
print meas,C


# COMMAND ----------

low1000,high1000

# COMMAND ----------

rdd1=rdd0.sample(False,0.01)\
         .map(lambda (key,val): val[1])\
         .cache()\
         .repartition(N)
rdd1.count()

# COMMAND ----------

tmp=computeOverAllDist(rdd1) 

# COMMAND ----------

tmp.keys(),tmp['mean'],tmp['std']

# COMMAND ----------

Clean_Tables={}
STAT={}
Names=['UnDef','mean','std','SortedVals','low100','high100','low1000','high1000']
STAT={}
for meas in measurements: #measurements:   #for each type of measurement, do
  rdd1=Tables[meas]
  STAT[meas]=computeOverAllDist(rdd1)   # Compute the statistics 
  print meas,STAT.keys()
  low1000 = STAT[meas]['low1000']  # unpack the statistics UnDef is an array holding the number of undefined in each row.
  high1000 = STAT[meas]['high1000']
                                                              
  rdd2=rdd1.map(lambda V: np.array([x if (x>low1000-1) and (x<high1000+1) else np.nan for x in V]))
  #print meas,'before removing rows',rdd2.count()
  # Remove entries that have 50 or more nan's
  rdd3=rdd2.filter(lambda row:sum(np.isnan(row))<50)
  #print meas,'after removing rows with too many nans',rdd3.count()
  Clean_Tables[meas]=rdd3.cache().repartition(N)
  C=Clean_Tables[meas].count()
  print meas,C
  

# COMMAND ----------

[(meas,len(STAT[meas])) for meas in measurements]

# COMMAND ----------

#[(meas,Clean_Tables[meas].count()) for meas in measurements]
Clean_Tables['TMAX']

# COMMAND ----------

#UnDef = an array that gives the number of nan's in each record
#mean,std = the mean and std of all non-nan measurements
#SortedVals = a sorted sample of values
#low100,high100 = bottom 1% and top 1% of the values
#low1000,high1000 = bottom .1% and top .1% of the values
Names=['UnDef','mean','std','SortedVals','low100','high100','low1000','high1000']
STAT={}
for meas in measurements:
  STAT[meas]={Names[i]:Statistics[meas][i] for i in range(len(Statistics[meas]))}
  print meas,STAT.keys()

# COMMAND ----------

from pickle import dumps
dbutils.fs.put("/mnt/OPEN-weather/Weather/TMAX_Statistics.pickle",dumps(TMAX_STAT),True)

# COMMAND ----------

UnDef,mean,std,SortedVals,low100,high100,low1000,high1000 =X
Filt_Vals = [s if s<]
print mean,std,low100,high100,low1000,high1000,len(UnDef),len(SortedVals)


# COMMAND ----------

rdd2=rdd1.map(lambda V: [x if (x>low1000) and (x<high1000) else np.nan for x in V]).cache()
rdd2.count()

# COMMAND ----------

import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.hist([x for x in SortedVals if x>low1000 and x<high1000],bins=50)
display(fig)

# COMMAND ----------

import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.hist(SortedVals,bins=36)
display(fig)


# COMMAND ----------

rdd2=Tables['TMAX'].map(lambda V: [x if (x>low1000) and (x<high1000) else np.nan for x in V]).cache()
print 'before removing rows',rdd2.count()
# Remove entries that have 50 or more nan's
rdd3=rdd2.filter(lambda row:sum(np.isnan(row))<50).cache()
print 'after removing rows with too many nans',rdd3.count()

# COMMAND ----------

rdd0.take(10)

# COMMAND ----------

groups=rdd0.groupByKey().cache()
groups.count()

# COMMAND ----------

groups1=groups.sample(False,0.01).collect()
#for group in groups1:
#  print group[0],len(group[1]) #[v[0] for v in group[1]]
groups2=[(g[0],[e for e in g[1]]) for g in groups1]
groups2[2]
len(groups2)         

# COMMAND ----------

#Wrote 468977060 bytes.
#Out[36]: True

from pickle import dumps
dbutils.fs.put("/mnt/NCDC-weather/Weather/SampleStations.pickle",dumps(groups2),True)

# COMMAND ----------

US_Weather_parquet='/mnt/NCDC-weather/Weather/US_Weather.parquet/'
df = sqlContext.sql("SELECT * FROM  parquet.`%s`  where measurement=='TMAX'"%US_Weather_parquet)
rdd0=df.map(lambda row:np.array([np.float64(row[str(i)]) for i in range(1,366)]))
rdd1=rdd0.cache() #.sample(False,0.1).cache()
print 'size of RDD=',rdd1.count()
# Remove entries that have 10 or more nan's
nanthr=50
rdd=rdd1.filter(lambda row:sum(np.isnan(row))<nanthr).cache()
print "size of rdd after removal of rows with more than %d nans = %d"%(nanthr,rdd.count())

# COMMAND ----------

import matplotlib.pyplot as plt
UnDef=rdd1.map(lambda row:sum(np.isnan(row))).collect()
fig, ax = plt.subplots()
ax.hist(UnDef,bins=36)
display(fig)



# COMMAND ----------

# Remove entries that have 10 or more nan's
rdd=rdd1.filter(lambda row:sum(np.isnan(row))<150).cache()
rdd.count()

# COMMAND ----------

# MAGIC %md ## compute covariance

# COMMAND ----------

OUT=computeCov(Clean_Tables['TMAX'])
OUT

# COMMAND ----------

len(OUT)

# COMMAND ----------

names=['E','NE','O','NO','Cov','Mean','Var']
COV_STAT={names[i]:OUT[i] for i in range(len(OUT))}
COV_STAT.keys()

# COMMAND ----------

from pickle import dumps
dbutils.fs.put("/mnt/OPEN-weather/Weather/COV_TMAX.pickle",dumps(COV_STAT),True)

# COMMAND ----------

(E,NE,O,NO,Cov,Mean,Var)=OUT
#shape(np.vstack([E,NE])),np.shape(Mean),Mean,Cov

# COMMAND ----------

type(O[0,0])

# COMMAND ----------

flat=np.reshape(NO,-1)
np.min(flat),np.max(flat)

# COMMAND ----------

fig=YearlyPlots(NE,'NE')
display(fig)

# COMMAND ----------

from numpy import linalg as LA
w,v=LA.eig(Cov)

# COMMAND ----------

COV_STAT['W']=w
COV_STAT['V']=v
[(key,shape(COV_STAT[key])) for key in COV_STAT.keys()]

# COMMAND ----------

fig=YearlyPlots(Mean,'Mean','Mean','TMAX')
display(fig)

# COMMAND ----------

from datetime import date
from numpy import shape
import matplotlib.pyplot as plt
import pylab as py
from pylab import ylabel,grid,title

dates=[date.fromordinal(i) for i in range(1,366)]
def YearlyPlots(T,ttl='',lbl='eigen',Ylabel='TMAX',size=(14,7)):
    fig=plt.figure(1,figsize=size,dpi=300)
    fig, ax = plt.subplots(1)
    if shape(T)[0] != 365:
        raise ValueError("First dimension of T should be 365. Shape(T)="+str(shape(T)))
    if len(shape(T))==1:
      ax.plot(dates,T[:],label=lbl);
    else:
      for i in range(shape(T)[1]):
        ax.plot(dates,T[:,i],label=lbl+' %d'%i);
    # rotate and align the tick labels so they look better
    fig.autofmt_xdate()
    ylabel(Ylabel)
    grid()
    ax.legend()
    title(ttl)
    return fig
fig=YearlyPlots(v[:,:3],'Eigen-Vectors')
display(fig)


# COMMAND ----------

print len(shape(v[:,:5])),len(shape(Mean))

# COMMAND ----------

def plot_reconstructions(selection,rows=2,columns=7):
    Recon=array(Eig*Prod.transpose()+Mean[:,np.newaxis])
    plt.figure(figsize=(columns*3,rows*3),dpi=300)
    j=0;
    for i in selection:
        subplot(rows,columns,j); 
        j += 1; 
        if j>=rows*columns: break
        plot(Recon[:,i])
        plot(Djoined.ix[i,1:365]);
        title(Djoined.ix[i,'station']+' / '+str(Djoined.ix[i,'year']))
        xlim([0,365])

# COMMAND ----------

Sample=df.sample(False,0.0001).collect()
len(Sample)

# COMMAND ----------

D={}
for key in Sample[0].asDict().keys():
  D[key]=[]
for j in range(1,len(Sample)):
  new=Sample[j].asDict()
  for key in D.keys():
    D[key].append(new[key])

# COMMAND ----------

import pandas as pd
DF=pd.DataFrame(D)
DF.head()

# COMMAND ----------

