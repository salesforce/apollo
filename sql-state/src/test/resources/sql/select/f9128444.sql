-- file:case.sql ln:72 expect:true
SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl
