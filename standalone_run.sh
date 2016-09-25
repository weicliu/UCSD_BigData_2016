export SPARK_HOME=/Library/spark-1.6.0-bin-hadoop2.6
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

jupyter notebook
