-- file:union.sql ln:226 expect:true
select from generate_series(1,5) union select from generate_series(1,3)
