-- file:aggregates.sql ln:742 expect:true
select my_avg(one),my_sum(two) from (values(1,2),(3,4)) t(one,two)
