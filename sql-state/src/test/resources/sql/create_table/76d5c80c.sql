-- file:insert.sql ln:118 expect:true
create table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb')
