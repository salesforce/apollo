-- file:collate.icu.utf8.sql ln:230 expect:true
SELECT a, b COLLATE "C" FROM collate_test1 UNION SELECT a, b FROM collate_test3 ORDER BY 2
