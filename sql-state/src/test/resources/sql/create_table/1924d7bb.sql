-- file:insert.sql ln:96 expect:true
create table part4 partition of range_parted for values from ('b', 10) to ('b', 20)
