select region_txt as "Region", count_if(success = 1) as successful_attempts, count(success) as total_attempts
from TERRORISMHOTSPOTS.TERRORISMHOTSPOTS.RAWINGEST
group by 1
order by 3 desc