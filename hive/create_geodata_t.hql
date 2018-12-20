
CREATE EXTERNAL TABLE geo(
	network_start_integer BigInt,
	network_last_integer BigInt,
	geoname_id Int,
	registered_country_geoname_id Int,
	represented_country_geoname_id Int,
	is_anonymous_proxy BOOLEAN,
	is_satellite_provider BOOLEAN
)
COMMENT 'Data from geolit2'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION '/user/cloudera/geodata/';