select gname as group_name, count_if(success = 1) as number_of_successful_attacks, count_if(success = 1)/count(success) as success_rate
from TERRORISMHOTSPOTS.TERRORISMHOTSPOTS.RAWINGEST
where gname != 'Unknown'
group by 1
order by 2 desc