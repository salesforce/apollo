-- file:insert.sql ln:48 expect:true
create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[])
