-- file:inherit.sql ln:633 expect:true
create table part_10_20_ab partition of part_10_20 for values in ('ab')
