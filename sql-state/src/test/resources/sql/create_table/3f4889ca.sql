-- file:insert.sql ln:172 expect:true
create table part_gg1 partition of part_gg for values from (minvalue) to (1)
