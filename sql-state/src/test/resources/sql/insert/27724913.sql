-- file:tablesample.sql ln:4 expect:true
INSERT INTO test_tablesample
  SELECT i, repeat(i::text, 200) FROM generate_series(0, 9) s(i)
