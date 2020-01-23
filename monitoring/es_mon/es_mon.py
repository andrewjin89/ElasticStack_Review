#-*- coding: utf-8 -*-

#Version : 1.1
import time
import sys
import os
import datetime
import dateutil.parser
from collections import OrderedDict

from elasticsearch import Elasticsearch
import configparser
from logging import handlers
import logging

import signal
import socket

SHUTDOWN = False

def handler(sigNum, frame):
  global SHUTDOWN
  SHUTDOWN = True
  logging.info("shutdown signum = %s"%sigNum)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGHUP, handler)
signal.signal(signal.SIGPIPE, handler)

class ElasticMon :

  def __init__(self, esIp, esPort, esId, esPwd, logDir, diskTh, heapTh, esCnt, esIndex, Logger) :
    self.esIp = esIp
    self.esPort = esPort
    self.esId = esId
    self.esPwd = esPwd
    self.logDir = logDir
    self.diskTh = int(diskTh)
    self.heapTh = int(heapTh)
    self.esCnt = int(esCnt)
    self.esIndex = esIndex
    self.Logger = Logger

  def es_check(self, es) :

    checkValue = True
    
    escheck = es.cat.health()

    escheck = escheck.split()[3]

    if escheck != "green" :
      checkValue = False

    return checkValue

  def config_index_check(self, es, esIndex) :

    checkValue = True

    try :
      
      indexcheck = es.cat.indices(index = esIndex)

    except :

      checkValue = False

    return checkValue
    #indexcheck = es.cat.indices(index = 'config-mysql-service')

  def config_index_create(self, es, esIndex, hostname, nowtime) :

    es.indices.create(
      index = esIndex,
      body = {
        "settings" : {
          "number_of_shards": 1,
          "number_of_replicas": 1
        },
        "mappings" : {
           "properties" : {
             "timestamp" : { "type" : "date" },
             "hostname" : { "type" : "keyword" }
           }
         }
       }
    )

    es.index(index = esIndex, id = 1, body = { "hostname" : hostname, "timestamp" : nowtime })

  def monitor_health_check(self, es, esIndex, hostname, nowtime) :

    es.indices.flush(index=self.esIndex)

    time.sleep(0.5)

    checkValue = True
    data = es.search(index = esIndex, body = { "query" : { "match_all" : {}}})

    checktime = nowtime - datetime.timedelta(minutes = 5)

    for hit in data['hits']['hits'] :
 
      monitor_hostname = hit["_source"]["hostname"]  
      monitor_date = hit["_source"]["timestamp"]

      mon_date = datetime.datetime.strptime(monitor_date[:19], "%Y-%m-%dT%H:%M:%S")

      if hostname != monitor_hostname : #ORG Setting
        if mon_date > checktime :       #ORG Setting
          print ("TEST1")
          es.index(index = esIndex, id = 1, body = { "hostname" : hostname, "timestamp" : nowtime })

        else :
          print ("TEST2")
          checkValue = False
      else :

        print ("TEST3")
        es.index(index = esIndex, id = 1, body = { "hostname" : hostname, "timestamp" : nowtime })

      print (checkValue ,  monitor_hostname, mon_date)
      return checkValue

  def disk_th_check(self, es, diskTh) :

    escheck = es.cat.allocation()

    for nodediskcheck in escheck.split('\n') :
      try :

        diskusage = int(nodediskcheck.split()[5])
        nodename = nodediskcheck.split()[6]

        if diskusage > diskTh :

          ### SMS Inert ###
          print ("Disk Usage Check. NodeName = %s, DiskUsage = %s : Disk Check Threshold (%s)" % (nodename, diskusage, diskTh))

      except :
        continue

  def heap_check(self, es, heapTh) :

    escheck = es.cat.nodes()

    for nodecheck in escheck.split('\n') :
      try :

        heapusage = int(nodecheck.split()[1])
        nodename = nodecheck.split()[9]
  
  #      print (type(heapusage))
  #      print (type(nodename))
        if heapusage > heapTh :
  
          ### SMS Inert ###
          print ("Heap Usage Check. NodeName = %s, HeapUsage = %s : Heap Check Threshold (%s)" % (nodename, heapusage, heapTh))
  
      except :
        continue

  def cluster_check(self, es, esCnt) :

    escheck = es.cat.health()

    for clustercheck in escheck.split('\n') :
      try :

        cluster = clustercheck.split()[3]

        if cluster == "green" :

          nodetotal = int(clustercheck.split()[4])
          unassign = int(clustercheck.split()[10])
          pending_tasks = int(clustercheck.split()[11])

          if nodetotal != esCnt :

            print ("Node Count Check. NodeCount = %s : Node Count Setting (%s)" % (nodetotal, esCnt))

          if unassign != 0 :

            print ("Node unassign Check. unassign = %s " % (unassign))

          if pending_tasks != 0 :

            print ("Node pending task Check. pending task = %s " % (pending_tasks))

      except :
        continue

  def run(self) :
    global SHUTDOWN

    self.Logger.info("INFO : elasticsearch host = %s elastisearch port = %s delete Index = %s" % (self.esIp, self.esPort, self.esIndex))

    try :

      es = Elasticsearch(hosts=[{'host':self.esIp, 'port':self.esPort}],http_auth=(self.esId, self.esPwd))

      while True :

        self.Logger.info("==========ElasticSearch Monitoring Start=========================================")

        hostname = socket.gethostname()
        nowtime = datetime.datetime.now()

        if SHUTDOWN:
          self.shutdown = True
          break

        self.Logger.info("ElasticSearch Check Start")

        escheck = self.es_check(es)

        if escheck == True :

          self.Logger.info("ElasticSearch Check Status = %s" % (escheck))
          
          indexcheck = self.config_index_check(es, self.esIndex)

          if indexcheck != False :

            self.Logger.info("ElasticSearch %s Index Check OK." % (self.esIndex))

            monhealthcheck = self.monitor_health_check(es, self.esIndex, hostname, nowtime)

            if monhealthcheck == True :

#              ##### Health Check #####
#
#              self.Logger.info("ElasticSearch Cluster Health Check Start.")
#
#              clustercheck = self.es_check(es)
#
#              self.Logger.info("ElasticSearch Cluster Health Check End.")

              #### Cluster Check ##### 

              self.Logger.info("ElasticSearch Cluster Check Start.")
              
              self.cluster_check(es, self.esCnt)

              self.Logger.info("ElasticSearch Cluster Check End.")

              ##### Disk Usage Check #####

              self.Logger.info("ElasticSearch Node Disk Check Start.")
              
              self.disk_th_check(es, self.diskTh)

              self.Logger.info("ElasticSearch Node Disk Check End.")

              #### Node Count / Heap Usage Check ##### 

              self.Logger.info("ElasticSearch Node Count Check Start.")
              
              self.heap_check(es, self.heapTh)

              self.Logger.info("ElasticSearch Node Count Check End.")

            elif monhealthcheck == False :

              print (monhealthcheck)

          elif indexcheck == False :
 
            self.config_index_create(es, self.esIndex, hostname, nowtime)
            self.Logger.info("ElasticSearch Config (%s) Index Create ." % (self.esIndex))
#            self.Logger.info("ElasticSearch %s Index Create" % (self.esIndex))

        else :
          print (escheck)

        self.Logger.info("==========ElasticSearch Monitoring End=========================================")
        self.Logger.info("sleep 60")
        time.sleep(10)

    except Exception as e:
        self.Logger.info(e)

    #print (self.esIp, self.esPort, self.esId, self.esPwd, self.logDir, self.esIndex)


def main() :

  if len(sys.argv) != 2 :
    print ('usage : %s type confFile' % ( sys.argv[0] ))
    sys.exit()
  
  confFile = sys.argv[1]
  
  cConfig  = configparser.ConfigParser()
  ret   = cConfig.read(confFile)
  
  esIp = cConfig.get("COMMON","ESIP")
  esPort = cConfig.get("COMMON","ESPORT")
  esId = cConfig.get("COMMON","ESID")
  esPwd = cConfig.get("COMMON","ESPWD")
  logDir = cConfig.get("COMMON","LOG_DIR")
  diskTh = cConfig.get("COMMON","DISK_THRESHOLD")
  heapTh = cConfig.get("COMMON","HEAP_THRESHOLD")
  esCnt = cConfig.get("COMMON","ELASTIC_COUNT")
  esIndex = cConfig.get("ELASTIC","ESINDEX")
#  esCnt = cConfig.get("ELASTIC","ESCNT")
  logName = ('%s.log'% os.path.basename(sys.argv[0]))
  logFile = logDir + '/' +logName

  LogFormatter = logging.Formatter('%(asctime)s,%(message)s')

  LogHandler = logging.handlers.RotatingFileHandler(filename=logFile,  encoding='utf-8', maxBytes=1000000, backupCount=5)

  LogHandler.setFormatter(LogFormatter)
  LogHandler.suffix = "%Y%m%d"
  
  Logger = logging.getLogger()
  Logger.setLevel(logging.DEBUG)
  Logger.addHandler(LogHandler)

  #Logger.info("testtest")

  if len(ret) == 0 :
    Logger.info('Not Found ConfigFile [%s]' % confFile)
    sys.exit(1)

  p = ElasticMon(esIp, esPort, esId, esPwd, logDir, diskTh, heapTh, esCnt, esIndex, Logger)
  p.run()

if __name__ == '__main__' : main()
