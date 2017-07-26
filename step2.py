'''
16:137:602:900C
Introduction to Cloud and Big Data Systems (Spring 2017)

Assignment 4: Spark Streaming Application - Online Processing
Maruthi Ayyappan – Aishwarya Gunde – Beethoven Plaisir
'''
# Importing Libraries
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time
import datetime

sc = SparkContext(appName="PythonStreaming")
ssc = StreamingContext(sc, (20))
ssc.checkpoint("hdfs://ip-172-31-72-124:9000/user/ma1306/check")

# String to List Function
def stringToList(s):
        tStamp, x, y, v = s.split(",")
        lst = [ (int(x),int(y)) , int(v)]
        return lst

# Variability function used to give difference (max-min)		
def variability(w0):
        w = list(w0)
        x = w[0][0]
        y = w[0][1]
        mx = w[1][0]
        mn = w[1][1]
        diff = mx-mn
        return [x,y, diff]

#Printing function for Easy Viewing of Data
def printing(dStreamRDD):
        tStamp = time.time()
        dtStamp = datetime.datetime.fromtimestamp(tStamp).strftime('%H_%M_%S')
        outputFile = "./outputs/variability_"+str(dtStamp)+".txt"
        pyList = dStreamRDD.collect()
        f = open(outputFile, 'w')
        for r in pyList:
                x = list(r)
                f.write("%i, %i, %i" %( int(r[0]), int(r[1]), int(r[2]) )+"\n" )
        f.close()

lines = ssc.textFileStream("hdfs://172.31.72.124:9000/user/ma1306/inputData/")
windowData1 = lines.map( (stringToList) ) #Map
windowData2 = windowData1
maxRead = windowData1.reduceByKeyAndWindow(max,60,20) #Reduce
minRead = windowData2.reduceByKeyAndWindow(min,60,20) #Reduce

diffData = maxRead.join(minRead)
var = diffData.map(variability)
var.pprint()
var.foreachRDD(printing)

ssc.start()
ssc.awaitTermination()