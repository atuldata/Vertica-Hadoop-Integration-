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
        data['job']['from-config-values'][0]['inputs'][0]['value']="mstr_datamart";
        data['job']['from-config-values'][0]['inputs'][3]['value']=self.table.get_table_column(table_name)
        data['job']['from-config-values'][0]['inputs'][4]['value']="rowId"
        data['job']['from-config-values'][0]['inputs'][6]['value']="select min(rowId), "+self.config['NUM_MAPPERS']+" as max from mstr_datamart.temp_"+table_name
        data['job']['to-config-values'][0]['inputs'][5]['value']=table_name
        data['job']['to-config-values'][0]['inputs'][3]['value']=self.config['SQOOP_COMPRESSION']
        data['job']['to-config-values'][0]['inputs'][2]['value']=self.config['OUTPUT_FILE_FORMAT']
        data['job']['driver-config-values'][0]['inputs'][0]['value']=self.config['NUM_MAPPERS']
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
        data['job']['from-config-values'][0]['inputs'][6]['value']="select min(rowId), "+self.config['NUM_MAPPERS']+" as max from mstr_datamart.temp_"+table_name
        data['job']['to-config-values'][0]['inputs'][5]['value']=table_name
        data['job']['to-config-values'][0]['inputs'][3]['value']=self.config['SQOOP_COMPRESSION']
        data['job']['to-config-values'][0]['inputs'][2]['value']=self.config['OUTPUT_FILE_FORMAT']
        data['job']['driver-config-values'][0]['inputs'][0]['value']=self.config['NUM_MAPPERS']
	data=json.dumps(data)
	self.logger.info("RUNNING: %s",data)
 	response = requests.put(self.env['API_STORAGE_GRID_HOST']+"/"+table_name, data=data)
        return response.json()
	
    def update_json_file_with_query(self,table_name,primary_id,primary_value,secondary_id,secondary_value):
        jsonFile = open("data.json", "r")
	data=json.load(jsonFile)
        data['job']['name']=table_name
        data['job']['from-config-values'][0]['inputs'][0]['value']="mstr_datamart"
        data['job']['from-config-values'][0]['inputs'][2]['value']="select * from mstr_datamart."+table_name +" where " + primary_id + " = "+ str(primary_value)+" "+ secondary_id + " = " + str(secondary_value) + " and " + " \${CONDITIONS}"
        data['job']['from-config-values'][0]['inputs'][3]['value']=self.table.get_table_column(table_name)
        data['job']['to-config-values'][0]['inputs'][5]['value']="%2Ftmp%2F"+"tmp_"+table_name
	jsonFile.close()
 	response = requests.post(self.env['API_STORAGE_GRID_HOST']+"/"+table_name, data=data)
	self.logger.info("RUNNING: %s",data)
        return response.json()
    
    def update_existing_job_with_query(self,table_name,primary_id,primary_value,secondary_id,secondary_value):
	response = requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name)
        data=response.json()
	print data
        data['job']['from-config-values'][0]['inputs'][2]['value']="select * from mstr_datamart."+table_name +" where " + primary_id + " = "+ str(primary_value)+" "+ secondary_id + " = " + str(secondary_value) + " and " + " \${CONDITIONS}"
        data['job']['from-config-values'][0]['inputs'][3]['value']=self.table.get_table_column(table_name)
        data['job']['to-config-values'][0]['inputs'][5]['value']="%2Ftmp%2F"+"tmp_"+table_name
	#print data
 	#response = requests.put(self.env['API_STORAGE_GRID_HOST'], data=data)
        output=response.json()
	self.logger.info("RUNNING: %s",data)
        return output

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
	    self.logger.info(job_status.json())
        job_status= requests.get(self.env['API_STORAGE_GRID_HOST']+"/"+table_name+"/status")
	self.logger.info(job_status.json())
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
