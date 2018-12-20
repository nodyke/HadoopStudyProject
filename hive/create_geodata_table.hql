CREATE TABLE geodata
row format delimited 
fields terminated by ',' 
STORED AS TEXTFILE AS
select g.network_start_integer,g.network_last_integer,c.geoname_id,regexp_replace(c.country_name, "\"", "") as country_name
from geo g join country_ru c on g.registered_country_geoname_id = c.geoname_id;