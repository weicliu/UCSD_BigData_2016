# coding: utf-8

# Name: Weichen Liu
# Email: wel174@ucsd.edu
# PID: A53100198
from pyspark import SparkContext
sc = SparkContext()

# In[3]:

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel

from pyspark.mllib.util import MLUtils


# ### Higgs data set

# ### As done in previous notebook, create RDDs from raw data and build Gradient boosting and Random forests models. Consider doing 1% sampling since the dataset is too big for your local machine

# In[6]:

# Read the file into an RDD
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/HIGGS/HIGGS.csv'
inputRDD=sc.textFile(path)

# In[7]:

# Transform the text RDD into an RDD of LabeledPoints
Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')]).map(lambda line: LabeledPoint(line.pop(0), line))


# ### Reducing data size

# In[31]:

Data1=Data.sample(False,0.1, seed=255).cache()
(trainingData,testData)=Data1.randomSplit([0.7,0.3],seed=255)
trainingData.cache()
testData.cache()

# ### Gradient Boosted Trees

# In[10]:

errors={}
for depth in [10]:
    model=GradientBoostedTrees.trainClassifier(trainingData, {}, numIterations=18, maxDepth=depth)
    errors[depth]={}
    dataSets={'train':trainingData,'test':testData}
    for name in dataSets.keys():  # Calculate errors on train and test sets
        data=dataSets[name]
        Predicted=model.predict(data.map(lambda x: x.features))
        LabelsAndPredictions=data.map(lambda lp: lp.label).zip(Predicted)
        Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
        errors[depth][name]=Err
    print depth,errors[depth]