-- file:insert.sql ln:257 expect:true
create table mlparted3 partition of mlparted for values from (1, 20) to (1, 30)
