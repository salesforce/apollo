-- file:insert.sql ln:95 expect:true
create table part3 partition of range_parted for values from ('b', 1) to ('b', 10)
