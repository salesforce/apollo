-- file:inherit.sql ln:679 expect:true
create table parted_minmax (a int, b varchar(16)) partition by range (a)
