-- file:collate.sql ln:147 expect:true
SELECT a, b FROM collate_test1 EXCEPT SELECT a, b FROM collate_test2 ORDER BY 2
