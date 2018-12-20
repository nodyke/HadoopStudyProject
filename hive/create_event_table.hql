
CREATE EXTERNAL TABLE event(
	product_name String,
	price BigInt,
	purchase_datetime String,
	category String,
	ip String
)
COMMENT 'Purchase event info'
 PARTITIONED BY(pd String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION '/user/cloudera/events/';