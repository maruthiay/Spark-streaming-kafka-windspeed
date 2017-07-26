import sys
import os
from pyspark.sql import SparkSession
from operator import add

spark = SparkSession\
    .builder\
    .appName("Python_Wind_Map")\
    .getOrCreate()


def getSum(w):
    x = int(w[0])
    y = int(w[1])
    w = int(w[2])

    return [(x,y),w]

def getCount(w):
    x = int(w[0])
    y = int(w[1])
    w = int(w[2])
    return [(x,y),1]

def getAvg(w0):
    w = list(w0)
    x = int(w[0][0])
    y = int(w[0][1])
    v = int(w[1][0])
    c = int(w[1][1])
    return [x,y,float(v/c)]

def stringToList(s):
    x, y, v = s.split(",")
    lst = [ int(x), int(y), int(v)]
    return lst

def printing(avg1):
        outputFile = "./Average.txt"
        f = open(outputFile, 'w')
        for r in avg1:
                f.write("%i, %i, %i" %( r[0], r[1], r[2] )+"\n"  )
        f.close()


dirName = sys.argv[1]
dirList = os.listdir(dirName)
try: os.system("hdfs dfs -rm -r "+dirName)
except: pass
os.system("hdfs dfs -put "+dirName )
for i in range(len(dirList)):
    fileName = dirName + "/" + dirList[i]
    inputFile = spark.read.text(fileName).rdd
    if i == 0:
            combinedFile = inputFile
    else:
            combinedFile = combinedFile.union(inputFile)

inFile = combinedFile.map(lambda x: x[0]).map( (stringToList) )

a = inFile.map(getSum)
b = inFile.map(getCount)
sum   = a.reduceByKey(add)
count = b.reduceByKey(add)
avgData = sum.join(count)
avglist = avgData.map(getAvg)
avg = avglist.collect()
printing(avg)
spark.stop()