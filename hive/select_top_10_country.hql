select countrySearch(ip) as country_name,sum(price) as s
from event
where countrySearch(ip) != 'NONE'
group by countrySearch(ip) 
order by s desc
limit 10;
