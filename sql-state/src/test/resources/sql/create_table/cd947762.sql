-- file:insert.sql ln:309 expect:true
create table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue)
