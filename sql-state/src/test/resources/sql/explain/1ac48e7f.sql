-- file:rowtypes.sql ln:110 expect:true
explain (costs off)
select thousand, tenthous from tenk1
where (thousand, tenthous) >= (997, 5000)
order by thousand, tenthous
