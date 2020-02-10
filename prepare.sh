rm -r /home/neeraj/mydata/hdfs/datanode/current/*
hadoop namenode -format
. /home/neeraj/hadoop-2.7.3/sbin/stop-all.sh
. /home/neeraj/hadoop-2.7.3/sbin/start-all.sh


