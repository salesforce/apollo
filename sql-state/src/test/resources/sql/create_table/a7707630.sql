-- file:insert.sql ln:175 expect:true
create table part_gg2_2 partition of part_gg2 for values from (5) to (10)
