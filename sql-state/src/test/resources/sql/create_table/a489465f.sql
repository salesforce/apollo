-- file:insert.sql ln:174 expect:true
create table part_gg2_1 partition of part_gg2 for values from (1) to (5)
