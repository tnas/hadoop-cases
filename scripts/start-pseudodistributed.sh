export HADOOP_CONF_DIR=/home/thiago/projects/hadoop-cases/conf/pseudo-distributed

rm -rf /tmp/hadoop*
rm -rf /home/thiago/hadoop/tmp/namenode/*
rm -rf /home/thiago/hadoop/tmp/datanode/*

hdfs namenode -format

start-dfs.sh 
#start-yarn.sh 
#mapred --daemon start historyserver

hdfs dfs -mkdir -p /user/thiago/input
hdfs dfs -mkdir -p /user/hive/warehouse

#hdfs dfs -mkdir -p input
#hdfs dfs -put /home/thiago/projects/hadoop-cases/cases/wordcount/input/* input
#yarn jar hadoop-wordcount.jar input output
#jps
