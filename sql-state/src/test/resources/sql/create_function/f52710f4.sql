-- file:create_aggregate.sql ln:46 expect:true
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable
