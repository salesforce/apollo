-- file:aggregates.sql ln:103 expect:true
explain (verbose, costs off)
select s1, s2, sm
from generate_series(1, 3) s1,
     lateral (select s2, sum(s1 + s2) sm
              from generate_series(1, 3) s2 group by s2) ss
order by 1, 2
