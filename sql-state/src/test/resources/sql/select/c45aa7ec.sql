-- file:int8.sql ln:170 expect:true
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 <> 456
