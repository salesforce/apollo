-- file:inherit.sql ln:637 expect:true
create table part_21_30_cd partition of part_21_30 for values in ('cd')
