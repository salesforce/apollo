-- file:gin.sql ln:11 expect:true
insert into gin_test_tbl select array[1, 2, g] from generate_series(1, 20000) g
