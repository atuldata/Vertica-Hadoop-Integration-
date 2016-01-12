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
	
    def create_secondary_table(self,table_name):
        sql= "create table IF NOT EXISTS " + table_name + " (secondary_id int)"
	self.logger.info(sql)
        self.db.executeSQL(sql)

    def populate_secondary_table(self, sec_table_name, secondary_id, table_name, primary_id, primary_value):
        # Insert records to the secondary table for the given month
        sql= "insert into " + sec_table_name+ " select distinct(" + secondary_id+ ") from " + table_name + " where " + primary_id + "= ?"
        row_count = self.db.executeSQLArgs(sql, (primary_value))
	self.logger.info("INSERT_DATA: Inserted %d rows for %s with value %s" (row_count, primary_id, primary_value))
        self.db.commit()

    def get_secondary_id(self, secondary_id, table_name, primary_id, primary_value):
        # Insert records to the secondary table for the given month
        sql= "select distinct(" + secondary_id+ ") from " + table_name + " where " + primary_id + "= ?"
        secondary_value= self.db.retrieveSQLArgs(sql, (primary_value))
	return secondary_value

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
        # rowID is created so the sqoop mappers are not skewed

        createSQL = "create table IF NOT EXISTS " + tempTable + " like " + table_name
        self.db.executeSQL(createSQL)
        alterSQL = "alter table " + tempTable + " add column rowId int not null"
        self.db.executeSQL(alterSQL)
        partitionSQL = "ALTER TABLE " + tempTable + "  PARTITION BY rowId REORGANIZE"
        self.db.executeSQL(partitionSQL)
	self.logger.info("CREATE TEMP TABLE: %s",table_name)

        print "CREATE: Created temporary table with row id... "

    def truncate_table(self, tempTable):
        # Clean up the the temp table for the next iteration...
        sql = "truncate table  " + tempTable
	self.logger.info("TRUNCATE TABLE: %s",tempTable)
        self.db.executeSQL(sql)
        print "TRUNCATE: Truncated temp table... "

    def pupulate_temp_table_secondary(self, tempTable, columns, sequence, numMappers, table_name, primary_id, primary_value, secondary_id,
                          secondary_value):
        sql = "insert into " + tempTable + "(rowId, " + columns + ") select " + sequence + ".nextval % ? ," + columns + " from " + table_name + " where " + primary_id + "= ? and " + secondary_id + " = ? "
        num_rows = self.db.executeSQLArgs(sql, (numMappers, primary_value, secondary_value))
        self.db.commit()
	self.logger.info("INSERT: Inserted %s rows to  temporary table for %s =%s   and %s = %s",
            num_rows, primary_id, primary_value, secondary_id, secondary_value)

    def pupulate_temp_table(self, tempTable, columns, sequence, numMappers, table_name, primary_id, primary_value):
        sql = "insert into " + tempTable + "(rowId, " + columns + ") select " + sequence + ".nextval % ? ," + columns + " from " + table_name + " where " + primary_id + "= ? limit 200000;"
        num_rows = self.db.executeSQLArgs(sql, (numMappers, primary_value))
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
	primary_id = self.get_primary_column(table_name)
        # get the the column names of the given table
        sql = "select column_name from v_catalog.columns where table_name = ? and column_name not in(?,'rowId')"
        cols = self.db.retrieveSQLArgs(sql, (table_name,primary_id[0][0]))
        columns = ','.join(str(v[0]) for v in cols)
        return primary_id[0][0]+","+columns
    
    def get_table_column_with_type(self, table_name):
	primary_id = self.get_primary_column(table_name)
        # get the the column names of the given table
        sql = "select column_name || ' ' || data_type from v_catalog.columns where table_name = ? and column_name not in(?,'rowId')"
        cols = self.db.retrieveSQLArgs(sql, (table_name,primary_id[0][0]))
        columns = ','.join(str(v[0]) for v in cols)
        sql = "select column_name || ' ' || data_type from v_catalog.columns where table_name = ? and column_name=?"
        primary = self.db.retrieveSQLArgs(sql, (table_name,primary_id[0][0]))
        return primary[0][0]+","+columns
    
    def generate_status_table(self, table_name):
	sql = "select export_objects('','"+table_name+"')"
        table_statement = self.db.retrieveSQL(sql)
        table_statement=table_statement[0][0]
        table_statement=table_statement.split(";")[0]
	primary_id = self.config['PRIMARY_ID']
	skip_last_months=self.config['SKIP_LATEST_MONTHS']
        print primary_id	
        # get the the column names of the given table
        #sql = "select  distinct "+primary_id+" from " + table_name
	sql="select "+primary_id+ " from (select  "+primary_id+", row_number() over(order by "+primary_id+" desc) rn from mstr_datamart.advt_supply_demand_geo_monthly_fact group by 1) A where rn>"+skip_last_months+";"
	self.logger.info("RUNNING: %s",sql)
        primary_value= self.db.retrieveSQL(sql)
	print primary_value
	
	for value in primary_value:
	  sql = "select 1 from sqoop_etl_status where table_name='"+table_name+"' and primary_partition_value='"+str(value[0])+"' and primary_partition_column='"+primary_id+"'"
          is_there = self.db.retrieveSQL(sql)
          if not is_there:
	    sql=" insert into sqoop_etl_status values('"+table_name+"','"+self.config['HIVE_DB']+"','"+str(datetime.datetime.now())+"',"+"null"+",'"+primary_id+"','"+str(value[0])+"','"+'f'+"',"+self.config['NUM_MAPPERS']+")"
	    print sql
	    self.logger.info("RUNNING: %s",sql)
            self.db.executeSQL(sql)
            self.db.commit();
	
    def get_temp_table_column(self, table_name):
	primary_id = self.get_primary_column(table_name)
        # get the the column names of the given table
        sql = "select column_name from v_catalog.columns where table_name = ?"
        cols = self.db.retrieveSQLArgs(sql, (table_name))
        columns = ','.join(str(v[0]) for v in cols)
        return columns

    def create_external_table(self, table_name):
	table_statement=self.get_table_column_with_type(table_name) +" )"
	table_statement=table_statement+" as  copy  from '"+self.env['HDFS_STORAGE_HOST']
	table_statement=table_statement+table_name+"/*' ON ANY NODE ORC"
        table_statement="CREATE EXTERNAL TABLE "+ table_name+"_ext ( "+table_statement 
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

