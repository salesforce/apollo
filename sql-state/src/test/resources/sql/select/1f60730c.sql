-- file:aggregates.sql ln:589 expect:true
select rank(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4)) v(x)
