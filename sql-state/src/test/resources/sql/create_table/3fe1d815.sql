-- file:insert.sql ln:412 expect:true
create table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue)
