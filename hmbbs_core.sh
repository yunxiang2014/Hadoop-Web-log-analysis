#!/bin/base

#get yesterday format string
#yesterday=`date --date='1 days ago' +%Y_%m_%d`
yesterday=$1

#upload logs to hdfs
hadoop fs -put /usr/local/apache_logs/access_${yesterday}.log  /hmbbs_logs

#cleaning data
hadoop jar /usr/local/apache_logs/cleaned.jar hmbbsCleaner /hmbbs_logs/access_${yesterday}.log  /hmbbs_cleaned/${yesterday}  1>/dev/null


#alter hive table and then add partition to existed table
hive -e "ALTER TABLE hmbbs ADD PARTITION(logdate='${yesterday}') LOCATION '/hmbbs_cleaned/${yesterday}';"

#create hive table everyday
hive -e "CREATE TABLE hmbbs_pv_${yesterday} AS SELECT COUNT(1) AS PV FROM hmbbs WHERE logdate='${yesterday}';"
hive -e "CREATE TABLE hmbbs_reguser_${yesterday} AS SELECT COUNT(1) AS REGUSER FROM hmbbs WHERE logdate='${yesterday}' AND INSTR(url,'member.php?mod=register')>0;"
hive -e "CREATE TABLE hmbbs_ip_${yesterday} AS SELECT COUNT(DISTINCT ip) AS IP FROM hmbbs WHERE logdate='${yesterday}';"
hive -e "CREATE TABLE hmbbs_jumper_${yesterday} AS SELECT COUNT(1) AS jumper FROM (SELECT COUNT(ip) AS times FROM hmbbs WHERE logdate='${yesterday}' GROUP BY ip HAVING times=1) e;"
hive -e "CREATE TABLE hmbbs_${yesterday} AS SELECT '${yesterday}', a.pv, b.reguser, c.ip, d.jumper FROM hmbbs_pv_${yesterday} a JOIN hmbbs_reguser_${yesterday} b ON 1=1 JOIN hmbbs_ip_${yesterday} c ON 1=1 JOIN hmbbs_jumper_${yesterday} d ON 1=1;"

#delete hive tables
hive -e "drop table hmbbs_pv_${yesterday};"
hive -e "drop table hmbbs_reguser_${yesterday};"
hive -e "drop table hmbbs_ip_${yesterday};"
hive -e "drop table hmbbs_jumper_${yesterday};"


#sqoop export to mysql
#sqoop export --connect jdbc:mysql://hadoop:3306/hmbbs --username root --password admin --table hmbbs_logs_stat --fields-terminated-by '\001' --export-dir '/user/hive/warehouse/hmbbs_${yesterday}'

#delete hive tables
#hive -e "drop table hmbbs_${yesterday};"

