-- file:plpgsql.sql ln:1543 expect:true
SELECT * FROM test_ret_set_rec_dyn(5) AS (a int, b numeric, c text)
