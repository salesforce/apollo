-- file:aggregates.sql ln:739 expect:true
select my_avg(one) filter (where one > 1),my_sum(one) from (values(1),(3)) t(one)
