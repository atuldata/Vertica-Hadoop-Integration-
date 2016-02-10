SCRIPT_DIR=`dirname $0`
. $SCRIPT_DIR/pause_include.sh

# Alert if we have been paused too long.
alert=1

if [ $alert = 1 ] ; then
  pause_alert
fi

dayOfWeek=`date +%u`
hour=`date +%H`
minute=`date +%M`

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_advt_domain_cat_daily.yaml

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_advt_revshare_daily.yaml

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_geo_lucid_daily_storage.yaml

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_geo_lucid_daily.yaml 

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_pub_ch_advt_rev_daily.yaml

pause_run /var/feeds/sqoop_etl/sqoop_etl.py agg_pub_revshare_daily.yaml


