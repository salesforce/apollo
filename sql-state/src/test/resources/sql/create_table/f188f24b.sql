-- file:collate.sql ln:149 expect:true
CREATE TABLE test_u AS SELECT a, b FROM collate_test1 UNION ALL SELECT a, b FROM collate_test2
