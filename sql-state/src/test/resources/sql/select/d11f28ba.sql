-- file:inet.sql ln:123 expect:true
SELECT i, i + 500 AS "i+500" FROM inet_tbl
