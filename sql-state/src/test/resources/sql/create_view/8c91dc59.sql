-- file:collate.sql ln:94 expect:true
CREATE VIEW collview1 AS SELECT * FROM collate_test1 WHERE b COLLATE "C" >= 'bbc'
