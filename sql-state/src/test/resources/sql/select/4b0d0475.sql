-- file:aggregates.sql ln:733 expect:true
select my_avg(distinct one),my_sum(distinct one) from (values(1),(3),(1)) t(one)
