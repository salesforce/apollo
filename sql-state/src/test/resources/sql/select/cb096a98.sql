-- file:collate.sql ln:142 expect:true
SELECT a, b FROM collate_test1 UNION ALL SELECT a, b FROM collate_test2 ORDER BY 2
