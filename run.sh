
export SPARK_PATH=/Library/spark-1.6.0-bin-hadoop2.6
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
# Uncomment next line if the default python on your system is python3
# export PYSPARK_PYTHON=python3
$SPARK_PATH/bin/pyspark --master local[2]
