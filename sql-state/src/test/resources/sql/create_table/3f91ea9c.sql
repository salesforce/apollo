-- file:insert.sql ln:94 expect:true
create table part2 partition of range_parted for values from ('a', 10) to ('a', 20)
