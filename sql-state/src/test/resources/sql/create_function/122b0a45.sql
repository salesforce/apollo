-- file:polymorphism.sql ln:564 expect:true
create function dfunc(a int DEFAULT 1.0, int DEFAULT '-1') returns int as $$
  select $1 + $2
