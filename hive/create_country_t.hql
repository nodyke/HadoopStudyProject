CREATE EXTERNAL TABLE country_ru(
	geoname_id Int,
	locale_code String,
	continent_code String,
	continent_name String,
	country_iso_code String,
	country_name String,
	is_in_european_union Boolean
)
COMMENT 'Data from geolite2'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION '/user/cloudera/country/';
