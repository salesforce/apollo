-- file:insert.sql ln:409 expect:true
create table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0)
