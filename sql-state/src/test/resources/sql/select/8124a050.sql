-- file:aggregates.sql ln:634 expect:true
select rank(3) within group (order by stringu1,stringu2) from tenk1
