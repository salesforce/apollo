-- file:collate.sql ln:144 expect:true
SELECT a, b FROM collate_test1 UNION SELECT a, b FROM collate_test2 ORDER BY 2
