-- file:rowtypes.sql ln:288 expect:true
select row_to_json(r) from (select q2,q1 from tt1 offset 0) r
