hadoop fs -rm -skipTrash /user/cloudera/libs/*
rm -f /home/cloudera/HiveLpHiveUdfProject-1.0-SNAPSHOT.jar
sudo -s cp /media/sf_Documents/lp-hadoop-project/hive/HiveUdfLpProject/target/HiveLpHiveUdfProject-1.0-SNAPSHOT.jar /home/cloudera
sudo -s chmod 777 /home/cloudera/HiveLpHiveUdfProject-1.0-SNAPSHOT.jar
hadoop fs -put /home/cloudera/HiveLpHiveUdfProject-1.0-SNAPSHOT.jar /user/cloudera/libs/hive_udfs.jar