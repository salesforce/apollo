-- file:tablesample.sql ln:8 expect:true
SELECT id FROM test_tablesample TABLESAMPLE SYSTEM (100.0/11) REPEATABLE (0)
