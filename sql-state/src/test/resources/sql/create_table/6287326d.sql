-- file:triggers.sql ln:1309 expect:true
create table parted_stmt_trig1 partition of parted_stmt_trig for values in (1)
