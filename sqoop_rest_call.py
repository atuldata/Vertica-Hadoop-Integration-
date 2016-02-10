import requests
import json

#url = 'http://xvaa-i09.xv.dc.openx.org:12000/sqoop/v1/job'
url='http://storage-grid-gateway.openx.org:12000/sqoop/v1/job'
jsonFile = open("new.json", "r+")
data = json.load(jsonFile)
r=json.dumps(data)

print r

#url ='http://xvaa-i09.xv.dc.openx.org:12000/sqoop/v1/job/advt_supply_demand_geo_monthly_fact/start'
#url='http://xvaa-i09.xv.dc.openx.org:12000/sqoop/v1/job/23/status'
#url='http://xvaa-i09.xv.dc.openx.org:12000/sqoop/v1/job/23 - [PUT] - Update Job'
response = requests.delete(url+"/agg_advt_domain_cat_daily")
#response = requests.put(url+"/agg_advt_domain_cat_daily",data=r)

print response.json()

