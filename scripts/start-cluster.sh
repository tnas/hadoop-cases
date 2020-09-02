export HADOOP_CONF_DIR=/home/thiago/projects/hadoop-cases/conf/cluster

rm -rf /tmp/hadoop*
rm -rf /home/thiago/hadoop/tmp/namenode/*
rm -rf /home/thiago/hadoop/tmp/datanode/*

ssh onagro 'rm -rf /tmp/*'
ssh onagro 'rm -rf /home/thiago/hadoop/tmp/datanode/*'

hdfs namenode -format

start-dfs.sh 
start-yarn.sh 
mapred --daemon start historyserver

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/thiago
hdfs dfs -mkdir input
hdfs dfs -put /home/thiago/projects/hadoop-cases/cases/wordcount/input/* input

jps
