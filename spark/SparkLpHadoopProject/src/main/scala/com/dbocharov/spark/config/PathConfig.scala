package com.dbocharov.spark.config

object PathConfig {
  val HDFS_EVENT_PATH = "hdfs://localhost/user/cloudera/events/*/*/*/*"
  val HDFS_GEODATA_PATH = "hdfs://localhost/user/hive/warehouse/events.db/geodata/000000_0"
  val CHECKPOINT_PATH = "checkpoint_dir"
}
