from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import time

from utils import preprocess, get_RAM, draw_graphic, features, train


def run_app(is_optimized=False):
    spark = SparkSession.builder \
        .appName("Hadoop and Spark Lab") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark_context = spark.sparkContext
    times = []
    RAMs = []

    for i in range (100):
        print("STEP", i)
        start_time = time.time()

        df = spark.read.csv("hdfs://namenode:9001/polina/dataset/dataset.csv", header=True, inferSchema=True)
        if is_optimized:
            spark_context.parallelize(range(100)).map(train)
        else:
            train(df)

        end_time = time.time()

        RAMs.append(get_RAM(spark_context))
        times.append(end_time - start_time)

    draw_graphic(times, RAMs)
    spark.stop()


if __name__ == "__main__":
    run_app(is_optimized=True)
