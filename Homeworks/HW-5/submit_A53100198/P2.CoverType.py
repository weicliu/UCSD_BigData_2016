# coding: utf-8

# Name: Weichen Liu
# Email: wel174@ucsd.edu
# PID: A53100198
from pyspark import SparkContext
sc = SparkContext()

# In[1]:

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.util import MLUtils


# In[3]:

# Read the file into an RDD
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/covtype/covtype.data'
inputRDD=sc.textFile(path)


# ### Making the problem binary

# In[10]:

Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')]).map(lambda V:LabeledPoint(1.0, V[:-1]) if V[-1] == 2.0 else LabeledPoint(0.0, V[:-1])).cache()


# ### Reducing data size

# In[11]:

(trainingData,testData)=Data.randomSplit([0.7,0.3],seed=255)
trainingData.cache()
testData.cache()

# ### Gradient Boosted Trees

# In[13]:

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel

errors={}
for depth in [14]:
    model=GradientBoostedTrees.trainClassifier(trainingData, {}, numIterations=15, maxDepth=depth)
    errors[depth]={}
    dataSets={'train':trainingData,'test':testData}
    for name in dataSets.keys():  # Calculate errors on train and test sets
        data=dataSets[name]
        Predicted=model.predict(data.map(lambda x: x.features))
        LabelsAndPredictions=data.map(lambda lp: lp.label).zip(Predicted)
        Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
        errors[depth][name]=Err
    print depth,errors[depth]
