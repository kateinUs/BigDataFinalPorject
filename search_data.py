from __future__ import print_function
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *

lines = sc.textFile("/user/xz3369/csvname.txt")
listofpath = lines.collect()
spark = SparkSession.builder.appName("Python Spark Reader").config("spark.some.config.option", "some-value").getOrCreate()
res = []
i=0
while(i<len(listofpath)):
    df = spark.read.options(header='True').csv(listofpath[i])
    header = df.columns
    res.append((listofpath[i],header))
    i=i+1
file = open('findcol.txt','w')
file.write(str(res))
file.close()
