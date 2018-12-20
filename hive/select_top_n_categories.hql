set category_count = 5;
select category, count(*) as cnt
from event
group by category
order by cnt desc
limit ${hiveconf:category_count};
	

