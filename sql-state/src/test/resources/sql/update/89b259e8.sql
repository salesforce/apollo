-- file:update.sql ln:86 expect:true
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10
