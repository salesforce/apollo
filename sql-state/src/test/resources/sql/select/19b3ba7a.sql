-- file:union.sql ln:56 expect:true
SELECT f1 AS nine FROM FLOAT8_TBL
UNION
SELECT f1 FROM INT4_TBL
ORDER BY 1
