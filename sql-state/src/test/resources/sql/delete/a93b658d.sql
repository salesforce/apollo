-- file:join.sql ln:471 expect:true
DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a
