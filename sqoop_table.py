#!/opt/bin/python -u
# ETL Name      : sqoopVertica2Hive.py
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


class sqoop_table:
    def __init__(self,table_name,config,logger):
        self.logger = logger # use class name as the log name
        #  Reading configuration file ( YAML file )
	self.config=config
        self.env = yaml.load(open(config['ENV']))
        self.db = OnlineDB(self.env['DSN'], self.logger)
        set_schema_sql = self.env['SET_SCHEMA_SQL']
        self.db.executeSQL(set_schema_sql)
	
    def drop_table(self, table):
        sql = "drop table if exists " + table
	self.logger.info(sql)
        self.db.executeSQL(sql)
	
    def check_table(self, table):
        sql = "select table_name from v_catalog.tables where table_name= ?"
        result=self.db.retrieveSQLArgs(sql,(table))
	if not result:
	  return False
	elif result[0][0]==table:
	  return True
	
    def get_primary_column(self, table_name):
        # Select the month/day/primaryid which is not backed up yet
        sql = "select primary_partition_column from sqoop_etl_status where is_complete='f' and table_name='"+table_name+"' order by primary_partition_value LIMIT 1"
        primary_id = self.db.retrieveSQL(sql)
        if primary_id and primary_id[0][0]:
            print "SELECT: %s value selected for backup is %s" % (primary_id, primary_id[0][0])
        else:
            print "No new data available for copying"
        return primary_id

    def get_primary_value(self,table_name):
        sql = "select primary_partition_value from sqoop_etl_status where is_complete='f' and table_name='"+table_name+"' order by primary_partition_value LIMIT 1"
        primary_value = self.db.retrieveSQL(sql)
	return primary_value

    def get_row_count(self,table_name,primary_id,primary_value):
        sql = "select count(*) from "+table_name+" where "+ primary_id +" = "+ primary_value
        row_count = self.db.retrieveSQL(sql)
	return row_count

    def update_backup_status(self, table_name, primary_id, primary_value):
        # Insert records to the secondary table for the given month
	if primary_id:
          sql = "update sqoop_etl_status set is_complete='t', end_date = '" + str(datetime.datetime.now())+"' where primary_partition_column=? and primary_partition_value=? and table_name=?" 
          self.db.executeSQLArgs(sql, (primary_id,primary_value,table_name))
          self.db.commit()
	  self.logger.info("updated sqoop_etl status for table_name=%s",table_name)
          print "COMPLETED: Backed up the data for the %s = %s" % (primary_id, primary_value)

    def deleteCurrentSecondaryId(self, secTable, secID):
        # Since the sqoop job for this secondary id is completed we are deleting it
        sql = "delete from " + secTable + " where secondary_id= ?"
        self.db.executeSQLArgs(sql, (secID))
        self.db.commit()
        print "DELETE: Completed backup for the  secondary id %s" % (secID)

    def create_temp_table(self, tempTable, table_name):
        # Creating the temporary table with additional rowId column and partitioned on rowID
        createSQL = "create table IF NOT EXISTS " + self.config['DW_DB']+"."+tempTable + " like " + self.config['DW_DB']+"."+table_name
        self.db.executeSQL(createSQL)
        alterSQL = "alter table " +self.config['DW_DB']+"."+ tempTable + " add column rowId int not null"
        self.db.executeSQL(alterSQL)
        partitionSQL = "ALTER TABLE " + self.config['DW_DB']+"."+tempTable + "  PARTITION BY rowId REORGANIZE"
        self.db.executeSQL(partitionSQL)
	self.logger.info("CREATE TEMP TABLE: %s",self.config['DW_DB']+"."+table_name)

        print "CREATE: Created temporary table with row id... "

    def truncate_table(self, table_name):
        # Clean up the the temp table for the next iteration...
        sql = "truncate table  " + self.config['DW_DB']+"."+table_name
	self.logger.info("TRUNCATE TABLE: %s",table_name)
        self.db.executeSQL(sql)
        print "TRUNCATE: Truncated temp table... "

    def pupulate_temp_table(self, tempTable,sequence, numMappers, table_name, primary_id, primary_value):
	columns = self.get_temp_table_column(table_name)
	if primary_id:
          sql = "insert into " + self.config['DW_DB']+"."+ tempTable + "(rowId, " + columns + ") select " + sequence + ".nextval % ? ," + columns + " from " + self.config['DW_DB']+"."+table_name + " where " + primary_id + "= ? ;"
          num_rows = self.db.executeSQLArgs(sql, (numMappers, primary_value))
	else:
	  sql="insert into " + self.config['DW_DB']+"."+ tempTable + "(rowId, " + columns + ") select " + sequence + ".nextval % ? ," + columns + " from " + self.config['DW_DB']+"."+table_name + ""
          num_rows = self.db.executeSQLArgs(sql, (numMappers))
	print sql
        self.db.commit()
	self.logger.info("RUNNING: %s",sql)
    
    def backup_status(self,table_name):
        sql = "select 1 from sqoop_etl_status where is_complete='f' and table_name='"+table_name+"' order by primary_partition_value LIMIT 1"
        value= self.db.retrieveSQL(sql)
	if not value:
	  return "false"
	else:
	  return "true"
		
    def get_table_column(self, table_name):
        # get the the column names of the given table
        sql = "select column_name from v_catalog.columns where table_name = ? and table_schema='"+self.config['DW_DB']+"'"
        cols = self.db.retrieveSQLArgs(sql, (table_name))
        columns = ','.join(str(v[0]) for v in cols)
	print columns
        return columns
    
    def get_table_column_with_type(self, table_name):
	primary_id = self.get_primary_column(table_name)
        # get the the column names of the given table
        sql = "select column_name || ' ' || data_type from v_catalog.columns where table_name = ? and table_schema='"+self.config['DW_DB']+"'"
        cols = self.db.retrieveSQLArgs(sql, (table_name))
        columns = ','.join(str(v[0]) for v in cols)
	print columns
        return columns
    
    def generate_status_table(self, table_name):
	sql = "select export_objects('','"+self.config['DW_DB']+"."+table_name+"')"
        table_statement = self.db.retrieveSQL(sql)
        table_statement=table_statement[0][0]
        table_statement=table_statement.split(";")[0]
	primary_id = self.config['PRIMARY_ID']
	skip_last_months=self.config['SKIP_LATEST_MONTHS']
        # get the the column names of the given table
        #sql = "select  distinct "+primary_id+" from " + table_name
	sql="insert into sqoop_etl_status "
	if primary_id=="":
	  primary_id="NULL"
	  sql=sql+" select '"+table_name+"','"+self.config['HIVE_DB']+"',CAST('"+str(datetime.datetime.now())+"' AS timestamp),"+"null"+","+primary_id+","+primary_id+",'"+'f'+"',"+self.config['NUM_MAPPERS'] +" WHERE not exists(select 1 from sqoop_etl_status where table_name='"+table_name+"')"
	else: 
	  sql=sql+" select '"+table_name+"','"+self.config['HIVE_DB']+"','"+str(datetime.datetime.now())+"',"+"null"+",'"+primary_id+"',"+primary_id+",'"+'f'+"',"+self.config['NUM_MAPPERS']+" from (select  "+primary_id+", row_number() over(order by "+primary_id+" desc) rn from (select distinct "+primary_id+" from "+self.config['DW_DB']+"."+table_name+" ) C group by 1) A where rn>"+skip_last_months+" and not exists(select 1 from sqoop_etl_status where primary_partition_value="+primary_id+");"
	self.logger.info("RUNNING: %s",sql)
        self.db.executeSQL(sql)
        self.db.commit();
	
    def get_temp_table_column(self, table_name):
	primary_id = self.get_primary_column(table_name)
        # get the the column names of the given table
        sql = "select column_name from v_catalog.columns where table_name = ? and table_schema='"+self.config['DW_DB']+"'"
        cols = self.db.retrieveSQLArgs(sql, (table_name))
        columns = ','.join(str(v[0]) for v in cols)
        return columns

    def create_external_table(self, table_name):
	table_statement=self.get_table_column_with_type(table_name) +" )"
	table_statement=table_statement+" as  copy  from '"+self.env['HDFS_STORAGE_HOST']
	table_statement=table_statement+table_name+"/*' ON ANY NODE ORC"
        table_statement="CREATE EXTERNAL TABLE " + self.config['DW_DB']+"."+ table_name+"_ext ( "+table_statement 
        self.db.executeSQL(table_statement)
	self.logger.info("RUNNING: %s",table_statement)
        print "TRUNCATE: Truncated temp table... "

    def get_secondary_value(self, sec_table_name):
        sql = "select secondary_id from " + sec_table_name + " limit 1"
        secondary_value = self.db.retrieveSQL(sql)
        if secondary_value:
            print "SELECT: The secondary id that will be backed up next is %s" % (secondary_value[0][0])
        else:
            print "SELECT: Completed all secondary ids"

        return secondary_value

