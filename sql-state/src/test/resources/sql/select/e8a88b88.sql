-- file:aggregates.sql ln:730 expect:true
select my_avg(one),my_sum(one) from (values(1),(3)) t(one)
