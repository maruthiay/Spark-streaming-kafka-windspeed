'''
16:137:602:900C
Introduction to Cloud and Big Data Systems (Spring 2017)

Assignment 4: Generating Custom Input DataStream
Maruthi Ayyappan – Aishwarya Gunde – Beethoven Plaisir
'''

# Importing Libraries
import random
import sys
import os
import datetime
import time

endTime = datetime.datetime.now() + datetime.timedelta(minutes=15) #Stops after 15 mins of generating inputs for every 20 secs
while True:
        if datetime.datetime.now() >= endTime: break
        dt = datetime.datetime.now().strftime("_%H_%M_%S")
        fileName = "./inputs/inputData%s.txt"%(dt) #Stores the data in local inputData directory
        f = open(fileName, 'w')
        for x in range(100):
                for y in range(100):
                        t = random.randint(111111,999999)
                        z = random.randint(-100,100)
                        ln = "%i, %i, %i, %i" %(t,x,y,z) +"\n"
                        f.write(ln)
        f.close()
        os.system("hdfs dfs -put -f ./%s /user/ma1306/inputData/" %(fileName)) # Transfers from local to HDFS
        time.sleep(20) #Sleep time 20 seconds

