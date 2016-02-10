#!/opt/bin/python -u

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

class sqoop_json:
    def __init__(self,table_name,config,logger):
        self.logger = logger  # use class name as the log name
	self.table=sqoop_table(table_name,config,logger)
	self.config=config
	 #  Reading configuration file ( YAML file )
        self.env = yaml.load(open(config['ENV']))

    def update_json_file(self,table_name):
        jsonFile = open("data.json", "r+")
        data = json.load(jsonFile)
        data['job']['name']=table_name
        data['job']['from-config-values'][0]['inputs'][1]['value']="temp_"+table_name
        data['job']['from-config-values'][0]['inputs'][0]['value']=self.config['DW_DB'];
        data['job']['from-config-values'][0]['inputs'][3]['value']=self.table.get_table_column(table_name)
        data['job']['from-config-values'][0]['inputs'][4]['value']="rowId"
        data['job']['from-config-values'][0]['inputs'][6]['value']="select min(rowId), "+self.config['NUM_MAPPERS']+" as max from "+self.config['DW_DB']+".temp_"+table_name
        data['job']['to-config-values'][0]['inputs'][5]['value']=table_name
        data['job']['to-config-values'][0]['inputs'][3]['value']=self.config['SQOOP_COMPRESSION']
        data['job']['to-config-values'][0]['inputs'][2]['value']=self.config['OUTPUT_FILE_FORMAT']
        data['job']['driver-config-values'][0]['inputs'][0]['value']=self.config['NUM_MAPPERS']
        data['job']['from-link-id']=6
        data['job']['to-link-id']=10
	data=json.dumps(data)
	print data
	self.logger.info("RUNNING: %s",data)
 	response = requests.post(self.env['API_STORAGE_GRID_HOST']+"/"+table_name, data=data)
        return response.json()
	
    def update_existing_job_without_query(self,table_name):
	response = requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name)
        data=response.json()
        data['job']['from-config-values'][0]['inputs'][3]['value']=self.table.get_table_column(table_name)
        data['job']['from-config-values'][0]['inputs'][4]['value']="rowId"
        data['job']['from-config-values'][0]['inputs'][6]['value']="select min(rowId), "+self.config['NUM_MAPPERS']+" as max from "+self.config['DW_DB']+".temp_"+table_name
        data['job']['from-config-values'][0]['inputs'][0]['value']=self.config['DW_DB'];
        data['job']['to-config-values'][0]['inputs'][5]['value']=table_name
        data['job']['to-config-values'][0]['inputs'][3]['value']=self.config['SQOOP_COMPRESSION']
        data['job']['to-config-values'][0]['inputs'][2]['value']=self.config['OUTPUT_FILE_FORMAT']
        data['job']['driver-config-values'][0]['inputs'][0]['value']=self.config['NUM_MAPPERS']
        data['job']['from-link-id']=6
        data['job']['to-link-id']=10
	data=json.dumps(data)
	self.logger.info("RUNNING: %s",data)
 	response = requests.put(self.env['API_STORAGE_GRID_HOST']+"/"+table_name, data=data)
	self.logger.info("RESPONSE: %s",response.json())
        return response.json()
	
    def start_job(self,table_name):
        response = requests.put(self.env['API_STORAGE_GRID_HOST']+"/"+table_name+"/start")
        job_status= requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name+"/status")
        job_status=job_status.json()
	
	self.logger.info("STARTING API CALL for job %s",table_name)
        while (job_status['submission']['status']=="RUNNING" or job_status['submission']['status']=="BOOTING"):
          time.sleep(5)
          job_status= requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name+"/status")
          job_status=job_status.json()
          if job_status['submission']['status']=="SUCCEEDED":
            break
          elif job_status['submission']['status']=="FAILED":
	    self.logger.info("job %s has failed",table_name)
        job_status= requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name+"/status")
	self.logger.info("JOB STATUS", job_status.json())
   	print job_status.json()
 
    def check_job(self,table_name):
        response = requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name)
        data=response.json()
	if 'message' in data.keys():
          if 'Invalid job' in data['message']:
            return 0
          else:
            return 1
	else:
	  return 1
