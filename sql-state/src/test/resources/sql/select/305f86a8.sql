-- file:aggregates.sql ln:540 expect:true
select (select count(*)
        from (values (1)) t0(inner_c))
from (values (2),(3)) t1(outer_c)
