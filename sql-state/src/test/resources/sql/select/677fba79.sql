-- file:plpgsql.sql ln:1542 expect:true
SELECT * FROM test_ret_set_rec_dyn(1500) AS (a int, b int, c int)
