-- file:strings.sql ln:84 expect:true
SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL
