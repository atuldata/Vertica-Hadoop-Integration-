#!/opt/bin/python -u
# ETL Name      : sqoop_etl.py
# Purpose       : To back up any table  data mentioned in the yaml configuration file  from Vertica cluster to Hive
import os
import sys
import yaml
import time
import datetime
from erutil.OnlineDB import OnlineDB
from erutil.JobLock import JobLock
from erutil.EtlLogger import EtlLogger
import requests
import json
from sqoop_table import sqoop_table
from sqoop_json import sqoop_json
sys.path.append('/usr/lib64/python2.6/site-packages/')
import pyhs2


class sqoop_pyhive:
    def __init__(self,table_name,config,logger):
	self.logger = logger  # use class name as the log name
        self.env = yaml.load(open(config['ENV']))
        self.table = sqoop_table(table_name,config,logger)
        self.json = sqoop_json(table_name,config,logger)
	self.config=config

    def create_flat_table(self, table_name):
	table_statement=self.table.get_table_column_with_type(table_name) +" )"
        table_statement=table_statement +" ROW FORMAT DELIMITED" 
	table_statement=table_statement+" FIELDS TERMINATED BY ',' LINES TERMINATED BY "
	table_statement=table_statement+" '\\n' STORED AS TEXTFILE LOCATION '/user/sqoop2/"+table_name+"/'"
        table_statement=table_statement.replace("numeric","decimal")
        table_statement=table_statement.replace("timestamp(6)","timestamp")
	table_statement="CREATE EXTERNAL TABLE " +self.config['HIVE_DB']+"."+ table_name+"_text ("+table_statement

	self.logger.info("RUNNING at HIVE: %s",table_statement)
        with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user='sqoop2',password='',database=self.config['HIVE_DB']) as conn:
          with conn.cursor() as cur:
            cur.execute(table_statement)

    def create_original_table(self, table_name):
	table_statement=self.table.get_table_column_with_type(table_name) +" )"
        #if table_statement.find("PARTITION BY") > 0:
          #partition_column=table_statement.split("PARTITION BY")[1].replace(table_name+".","").replace("(","").replace(")","").replace(" ","")
	  #sql = "select data_type from v_catalog.columns where column_name= '"+partition_column+"' and table_name='"+table_name+"'"
          #data_type = self.db.retrieveSQL(sql)
	  #data_type=data_type[0][0]
          #table_statement=table_statement.split("PARTITION BY")[0]
	  #table_statement=first_part+ "PARTITIONED BY ("+ partition_column+ " "+data_type +" ) " 
	  #table_statement=table_statement.replace(partition_column+ " "+data_type+",","") 
	   
	table_statement=table_statement+" ROW FORMAT DELIMITED STORED AS ORC tblproperties ('orc.compress'='"+self.config['SQOOP_COMPRESSION']+"','orc.stripe.size'='"+self.config['ORC_STRIPE_SIZE']+"','orc.row.index.stride'='"+self.config['ORC_INDEX_STRIDE']+"','orc.create.index'='true')"
	table_statement=table_statement.replace("\n"," ")
	table_statement=table_statement.replace("numeric","decimal")
        table_statement=table_statement.replace("timestamp(6)","timestamp")
	table_statement="CREATE EXTERNAL TABLE " +self.config['HIVE_DB']+"."+ table_name + " ( " + table_statement
	 
        print table_statement

	self.logger.info("RUNNING at HIVE: %s",table_statement)
	with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user=self.env['USER_NAME'],password='',database=self.config['HIVE_DB']) as conn:
  	  with conn.cursor() as cur:
	    cur.execute(table_statement) 
	    print cur.getDatabases()

    def load_data_orc(self,table_name):
	 with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user=self.env['USER_NAME'],password='',database=self.config['HIVE_DB']) as conn:
          with conn.cursor() as cur:
	    #set_hive="SET hive.mergejob.maponly=true"
	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)

	    #set_hive=" SET hive.merge.mapredfiles=true"
	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)

	    #set_hive="SET hive.merge.mapfiles=true"
	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)
	
	    #set_hive="SET hive.merge.size.per.task=256000000"
	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)

	    #set_hive="SET hive.merge.smallfiles.avgsize=16000000000"
	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)

	    #self.logger.info("RUNNING at HIVE: %s",set_hive)
            #cur.execute(set_hive)
	    
	    hive_sql="INSERT INTO TABLE  "+self.config['HIVE_DB']+"."+table_name
	    #+" PARTITION ("+primary_id+"="+primary_value+")"
	    hive_sql=hive_sql+" SELECT "+self.table.get_table_column(table_name)+" from "+self.config['HIVE_DB']+"."+table_name+"_text"
	    print hive_sql
	    self.logger.info("RUNNING at HIVE: %s",hive_sql)
            cur.execute(hive_sql)

    def drop_flat_files(self,table_name):
         with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user=self.env['USER_NAME'],password='',database=self.config['HIVE_DB']) as conn:
          with conn.cursor() as cur:
            hive_sql="DROP TABLE "+self.config['HIVE_DB']+"."+table_name+"_text"
	    self.logger.info("RUNNING at HIVE: %s",hive_sql)
            cur.execute(hive_sql)

    def drop_files(self,table_name):
         with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user='sqoop2',password='',database=self.config['HIVE_DB']) as conn:
          with conn.cursor() as cur:
            hive_sql="dfs -rmr -skipTrash /user/sqoop2/"+table_name+"/*"
	    self.logger.info("RUNNING at HIVE: %s",hive_sql)
            cur.execute(hive_sql)
	
    def check_if_table_exists(self,table_name):
         with pyhs2.connect(host=self.env['HIVE_HOST'],port=10000,authMechanism="PLAIN", user=self.env['USER_NAME'],password='',database=self.config['HIVE_DB']) as conn:
          with conn.cursor() as cur:
            hive_sql="SHOW TABLES LIKE '"+table_name+"'"
            cur.execute(hive_sql)
	    return cur.fetch()
            
	
