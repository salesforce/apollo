-- file:aggregates.sql ln:622 expect:true
select rank(x) within group (order by x) from generate_series(1,5) x
