-- file:union.sql ln:72 expect:true
SELECT CAST(f1 AS char(4)) AS three FROM VARCHAR_TBL
UNION
SELECT f1 FROM CHAR_TBL
ORDER BY 1
