-- file:update.sql ln:44 expect:true
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j
