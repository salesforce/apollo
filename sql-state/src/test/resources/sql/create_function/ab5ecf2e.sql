-- file:polymorphism.sql ln:533 expect:true
create function dfunc(a int = 1, int = 2) returns int as $$
  select $1 + $2
