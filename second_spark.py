"""SimpleApp.py"""
from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("behrooz").setMaster("local")
sc = SparkContext(conf=conf)

# data = [1, 2, 3, 4, 5]
# distData = sc.parallelize(data)

# ddd = distData.reduce(lambda a, b: a + b)

# distFile = sc.textFile("actions.csv")

# ddd = distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)
# ddd.persist() # If we also wanted to use it again later

def sss(s):
	fff = s.split(" ")
	return fff

def intersection(lst): 
    lst1 = lst[0]
    lst2 = lst[1]
    lst3 = [value for value in lst1 if value in lst2] 
    return len(lst3)

# distFile = sc.textFile("testtest.dat")
# ddd0 = distFile.map(sss)
# ddd0.collect()
# print ddd0.collect()
# ddd1 = distFile.map(sss).cartesian(ddd0).map(intersection)
# ddd1 = distFile.cartesian(ddd0)
# ddd.reduce(lambda a, b: len(a) + len(b))
# print ddd1.collect()

hhh = ['a','a','b','a','b','c','c','a','b']
yyy = sc.parallelize(hhh)
counts = yyy.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print counts.collect()

# logFile = "actions.csv"  # Should be some file on your system
# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# logData = spark.read.text(logFile).cache()

# numAs = logData.filter(logData.value.contains('Ward')).count()
# numBs = logData.filter(logData.value.contains('b')).count()

# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

# spark.stop()