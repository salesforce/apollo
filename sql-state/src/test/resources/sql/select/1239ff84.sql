-- file:aggregates.sql ln:543 expect:true
select (select count(*) filter (where outer_c <> 0)
        from (values (1)) t0(inner_c))
from (values (2),(3)) t1(outer_c)
