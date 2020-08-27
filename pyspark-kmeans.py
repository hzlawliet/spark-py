#!/usr/bin/env python
#coding:utf-8

import sys
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import Vectors
import numpy as np

pt = sys.argv[1]

def result_process(data):
    if data[-1] in sensitivity_1:
        res = 1
    elif data[-1] in sensitivity_3:
        res = 3
    else:
        res = 2
    return '\t'.join([str(d) for d in data[:-5]] + [str(data[-1])] + [str(res)])

def process(sc):
    hiveContext = HiveContext(sc)
    hql = "select * from kmeans_cluster_feature where pt = '%s'" % (pt)
    df_raw = hiveContext.sql(hql).repartition(160)
    columns = df_raw.columns[1: -2]    
    feature_num = len(columns)
    # type
    #df_tmp = df_raw
    #for k, i in zip(columns, range(feature_num)):
    #    df_tmp = df_tmp.withColumn(k, df_tmp[i + 1] * 1.0)
    # Imputer
    mean_value = df_raw.describe().collect()[1]
    print mean_value
    df_train = df_raw
    for k, i in zip(columns, range(feature_num)):
        df_train = df_train.na.fill({k:mean_value[i + 1]})
    # minmax
    vecAssembler = VectorAssembler(inputCols=columns, outputCol="features")
    df_b_s = vecAssembler.transform(df_train)
    mmScaler = MinMaxScaler(inputCol="features", outputCol="scaled")
    model = mmScaler.fit(df_b_s)
    df_scaled = model.transform(df_b_s)
    # kmeans
    n_clusters_ = 20
    model = KMeans(k=n_clusters_, initSteps=10, maxIter=300, featuresCol='scaled').fit(df_scaled)
    df_result = model.transform(df_scaled)
    # map
    global sensitivity_1, sensitivity_3
    sensitivity_1 = []
    sensitivity_2 = []
    sensitivity_3 = []
    key_cnt = []
    centers = model.clusterCenters()
    for xx, yy in zip(centers, range(n_clusters_)):
        key_cnt.append([yy, xx[0]])
    sorted_cluster = sorted(key_cnt, key=lambda asd: asd[1])
    split = n_clusters_ / 3
    split_end = n_clusters_ - split
    for xx, yy in zip(sorted_cluster, range(n_clusters_)):
        if yy < split:
            sensitivity_3.append(xx[0])
        elif yy >= split_end:
            sensitivity_1.append(xx[0])
        else:
            sensitivity_2.append(xx[0])
    #result
    df_result.map(result_process).saveAsTextFile("kmeans_cluster_result/pt=%s/" % (pt))


if __name__ == "__main__":
    #spark = SparkSession.builder.appName("price_sensitivity_cluster").config("spark.port.maxRetries", "100").getOrCreate()
    #sc = spark.sparkContext
    conf = SparkConf().setAppName("price_sensitivity_cluster")
    conf.set("spark.port.maxRetries", 100)
    sc = SparkContext(conf=conf)
    process(sc)
    sc.stop()


