#!/opt/bin/python -u
# Python File      : sqoop_etl.py
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
from sqoop_pyhive import sqoop_pyhive

sys.path.append('/usr/lib64/python2.6/site-packages/')
import pyhs2

sys.path.append(os.path.realpath(sys.argv[0]))
config = yaml.load(open(sys.argv[1]))

class sqoop_etl:
    def __init__(self,table_name):
        self.logger = EtlLogger.get_logger(table_name)  # use class name as the log name
        self.lock = JobLock(table_name)  # use class name as the lock name
        #  Reading configuration file ( YAML file )
        self.env = yaml.load(open(config['ENV']))
	self.config=config
        self.table = sqoop_table(table_name,config,self.logger)
        self.json = sqoop_json(table_name,config,self.logger)
        self.pyhive = sqoop_pyhive(table_name,config,self.logger)

    def back_with_static_table(self, primary_id, primary_value, table_name):
        temp_table_name = "temp_" + table_name
        is_job = self.json.check_job(table_name)
        
	if not is_job:
            data = self.json.update_json_file(table_name)
            print data
        else:
            data = self.json.update_existing_job_without_query(table_name)
            print data
	return	
       	table=self.table.check_table(temp_table_name)
        
	columns = self.table.get_temp_table_column(table_name)
	if not table:
          self.table.create_temp_table(temp_table_name, table_name)
        
	self.table.truncate_table(temp_table_name)
	self.table.pupulate_temp_table(temp_table_name, columns, self.config["SEQUENCE"], self.config["NUM_MAPPERS"],
                                              table_name, primary_id[0][0],
                                               primary_value[0][0])
       	self.json.start_job(table_name)
	 
 	self.pyhive.load_data_orc(table_name,primary_id[0][0],primary_value[0][0])
		
	self.pyhive.drop_files(table_name)
        self.table.update_backup_status(table_name, primary_id[0][0], primary_value[0][0])

    def backup_without_static_table(self, primary_id, primary_value, table_name):
        secondary_id_list = self.table.get_secondary_id(self.config["SECONDARY_ID"], table_name, primary_id[0][0],
                                                        primary_value[0][0])
        for secondary_value in secondary_id_list:
            is_job = self.json.check_job(table_name)
            if is_job == 0:
                data = self.json.update_json_file_with_query(table_name, primary_id[0][0], primary_value[0][0],
                                                             self.config["SECONDARY_ID"], secondary_value[0])
                print data
            else:
                data = self.json.update_existing_job_with_query(table_name, primary_id[0][0], primary_value[0][0],
                                                                self.config["SECONDARY_ID"], secondary_value[0])
                print data
	
	self.json.start_job(table_name)
        self.pyhive.load_data_orc(table_name,primary_id[0][0],primary_value[0][0])
        self.pyhive.drop_files(table_name)
        self.table.update_backup_status(table_name, primary_id[0][0], primary_value[0][0])

    def start_backup(self, table_name):
	self.table.generate_status_table(table_name)
        backup_records =self.table.backup_status(table_name)
	self.logger.info("backup records running:%s",backup_records)
	self.logger.info("backup job start time: %s",datetime.datetime.now())
        while (backup_records == "true"):
	
            primary_id = self.table.get_primary_column(table_name)
            primary_value = self.table.get_primary_value(table_name)
	    
	    if_table=self.pyhive.check_if_table_exists(table_name)
	    if not if_table:
	       self.pyhive.create_original_table(table_name)

	    if_flat_table=self.pyhive.check_if_table_exists(table_name+"_text")	
	    if not if_flat_table:
	       self.pyhive.create_flat_table(table_name)
	    
            self.back_with_static_table(primary_id, primary_value, table_name)
	    break
            backup_records =self.table.backup_status(table_name)
	
       	table=self.table.check_table(table_name+"_ext")
	if not table:
	   self.table.create_external_table(table_name)
	self.logger.info("backup job end time: %s",datetime.datetime.now())


if __name__ == "__main__":
    table_name = sys.argv[1].replace('.yaml', '')
    start_time = time.time()
    etl_instance = sqoop_etl(table_name)
    try:
        if not etl_instance.lock.getLock():
            etl_instance.logger.info("Unable to get lock, exiting...")
            sys.exit(0)
        etl_instance.logger.info("Start running DW to Storage Grdi backup script for table %s script %s",table_name, datetime.datetime.now())
        etl_instance.start_backup(table_name)

    except SystemExit:
        pass
    except:
        etl_instance.logger.error("Error: %s", sys.exc_info()[0])
        raise
    finally:
        etl_instance.lock.releaseLock()
