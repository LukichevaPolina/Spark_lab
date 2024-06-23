from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import FMRegressor
from pyspark.sql.functions import isnull, col, to_date, to_timestamp, year, month, dayofmonth

import matplotlib.pyplot as plt

import psutil
import time
import os


features = ["Order ID", "Units Sold", "Unit Price", "Unit Cost", \
            "Total Revenue", "Total Cost", "RegionEncoded", "CountryEncoded", \
            "Item TypeEncoded", "Sales ChannelEncoded", "Order PriorityEncoded", \
            "Order DateYear", "Order DateMonth", "Order DateDay", \
            "Ship DateYear", "Ship DateMonth", "Ship DateDay"]

def train(dataset):
    dataset = preprocess(dataset)
    dataset = VectorAssembler(inputCols=features, outputCol="features").transform(dataset)

    train_data, test_data = dataset.randomSplit([0.8, 0.2], seed=42)
    fm = FMRegressor(featuresCol="features", labelCol="Total Profit", maxIter=1)
    model = fm.fit(train_data)


def get_RAM(spark_context):
    executor_memory_status = spark_context._jsc.sc().getExecutorMemoryStatus()
    executor_memory_status_dict = spark_context._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(executor_memory_status).asJava()
    total_used_memory = 0
    for executor, values in executor_memory_status_dict.items():
        total_memory = values._1() / (1024 * 1024)
        free_memory = values._2() / (1024 * 1024)
        used_memory = total_memory - free_memory
        total_used_memory += used_memory
    return total_used_memory

def draw_graphic(times, RAMs):
    plt.figure(figsize=(14, 6))
    plt.subplot(1, 2, 1)
    plt.hist(times, bins=30, edgecolor='k', alpha=0.7)
    plt.xlabel('Time(c)')
    plt.ylabel('Frequency')
    plt.title('Histogram of time distribution')
    plt.grid(True)

    plt.subplot(1, 2, 2)
    plt.hist(RAMs, bins=30, edgecolor='k', alpha=0.7)
    plt.xlabel('RAM(MB)')
    plt.ylabel('Frequency')
    plt.title('Histogram of RAM distribution')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("/not_optim.png")

def handle_categorical_data(dataset):
    features = ["Region", "Country", "Item Type", "Sales Channel", "Order Priority"]

    indexers = [StringIndexer(inputCol=col, outputCol=col + "Index") for col in features]
    pipeline = Pipeline(stages=indexers)
    dataset_indexed = pipeline.fit(dataset).transform(dataset)

    for column in features:
        dataset_indexed = dataset_indexed.drop(column)

    encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=column + "Encoded") for indexer, column in zip(indexers, features)]
    pipeline = Pipeline(stages=encoders)
    dataset_encoded = pipeline.fit(dataset_indexed).transform(dataset_indexed)

    for column in features:
        dataset_encoded = dataset_encoded.drop(column + "Index")
    
    return dataset_encoded

def handle_dates(dataset):
    features = ["Order Date", "Ship Date"]
    for feature in features:
        dataset = dataset.withColumn(feature, to_date(to_timestamp(feature, "M/d/y")))
        dataset = dataset.withColumn(feature + "Year", year(dataset[feature]))
        dataset = dataset.withColumn(feature + "Month", month(dataset[feature]))
        dataset = dataset.withColumn(feature + "Day", dayofmonth(dataset[feature]))
        dataset = dataset.drop(feature)

    return dataset

def preprocess(dataset):
    dataset = dataset.na.fill(0)
    dataset = handle_categorical_data(dataset)
    dataset = handle_dates(dataset)

    return dataset 

