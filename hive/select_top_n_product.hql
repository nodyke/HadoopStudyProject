set product_count = 2;
select t.category,t.product_name,t.cnt
from
	(
		select category,product_name,count(*) as cnt,row_number() over(partition by category order by count(*) desc) as num
		from event
		group by category,product_name
	) t
where t.num <= ${hiveconf:product_count}



	

