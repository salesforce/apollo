-- file:triggers.sql ln:1314 expect:true
create table parted2_stmt_trig2 partition of parted2_stmt_trig for values in (2)
