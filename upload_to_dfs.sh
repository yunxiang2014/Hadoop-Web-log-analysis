#!/bin/base
#upload logs to hdfs

#get yesterday format string
#yesterday=`date --date='1 days ago' +%Y_%m_%d`
yesterday=$1
echo $yesterday

hadoop fs -put /usr/local/apache_logs/access_${yesterday}.log /hmbbs_logs

#cleaning date


hadoop jar /usr/local/apache_logs/cleaned.jar hmbbsCleaner  /hmbbs_logs/access_${yesterday}.log   /hmbbs_cleaned/${yesterday}
