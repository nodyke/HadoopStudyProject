drop table if exists top_country;
create table if not exists top_country
row format delimited 
fields terminated by ',' 
STORED AS TEXTFILE as
select countrySearch(ip) as country_name,sum(price) as s
from event
where countrySearch(ip) != 'NONE'
group by countrySearch(ip) 
order by s desc
limit 10;
drop table if exists top_category;
create table if not exists top_category
row format delimited 
fields terminated by ',' 
STORED AS TEXTFILE AS
select category, count(*) as cnt
from event
group by category
order by cnt desc
limit 3;
drop table if exists top_product;
create table if not exists top_product 
row format delimited 
fields terminated by ',' 
STORED AS TEXTFILE AS
select t.category,t.product_name,t.cnt
from
	(
		select category,product_name,count(*) as cnt,row_number() over(partition by category order by count(*) desc) as num
		from event
		group by category,product_name
	) t
where t.num <= 2;



	


	

