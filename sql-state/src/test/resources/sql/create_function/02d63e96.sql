-- file:polymorphism.sql ln:448 expect:true
create function first_el(anyarray) returns anyelement as
'select $1[1]' language sql strict immutable
