-- file:insert.sql ln:131 expect:true
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b)
