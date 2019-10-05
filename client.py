from __future__ import print_function

import time
import socket
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def ClientMain():
  
  #######################     Make connection     ###############################

  print ("starting Socket writing thread")
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Create an INET, STREAMing socket
  #s.bind(('localhost',9999))
  s.connect(('127.0.0.1',9999))
  sc = SparkContext(appName="PythonStreamingNetworkWordCount")

  #lines = sc.wholeTextFiles("texts/C01/*").values().flatMap(lambda line: line.replace(".\n", " ").replace(",", " ").replace(". ", " ").replace("(", " ").replace(")"," ").replace('"', ' ').replace("[", " ").replace("]", " ").replace(";", " ").replace(".", " ").split(' ')).collect()
  data = sc.wholeTextFiles("texts/C01/*")
  lines = data.values().collect()
  files = data.keys().collect()
  for (i,l) in enumerate(lines):
    rdd = sc.parallelize([l])
    words = rdd.flatMap(lambda line: line.replace(".\n", " ").replace(".\\n", " ").replace(",", " ").replace(". ", " ").replace("(", " ").replace(")"," ").replace('"', ' ').replace("[", " ").replace("]", " ").replace(";", " ").replace(".", " ").split(' '))\
                .map(lambda x: (x, 1))\
                .reduceByKey(lambda x, y: x + y)\
                .map(lambda (x, y): (y, x))\
                .sortByKey(ascending=False)
    output = words.collect()
    s.send("file : %s" % (files[i]))
    time.sleep(10)
    for (count,word) in output:
        s.send("%s: %i" % (word,count))
        print("%s: %i" % (word,count))
        #time.sleep(5)
  print ("Sending Complete")
  s.close() # close connection
  """
#  s.bind(('localhost',9999)) 
#  s.listen(5)

#  ss, addr = s.accept()
#  print ("connection ready.Sending data")
  
  s.connect(('localhost',9999)) 
  ########################     Read Folders      ################################
  parent_folder = "ohsumed-all/"

  parent_folder_list = os.listdir(parent_folder)
  print (parent_folder_list)
  
  print ("Total number of folders: " + str(len(parent_folder_list))) #20
  ###############################################################################
  
  
  
  for m in range(0,len(parent_folder_list)):
    subfolder =  parent_folder + parent_folder_list[m] + "/"
    print(subfolder)
    subfolder_list = os.listdir(subfolder)

    print ("Total number of text files:" + str(len(subfolder_list)))

    for n in range(0,len(subfolder_list)):
      fileForRead =  subfolder + subfolder_list[n]
      f = open(fileForRead, 'rb')
      while True:      
        for line in f:
          print(line) 
          s.send(line) #transmits TCP message
          time.sleep(2)
        break
      f.close() #close file
      
  print ("Sending Complete")
  s.close() # close connection

  """
if __name__ == '__main__':
  ClientMain()
