-- file:int8.sql ln:166 expect:true
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456
