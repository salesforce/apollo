-- file:polymorphism.sql ln:608 expect:true
create function dfunc(anyelement = 'World'::text) returns text as $$
  select 'Hello, ' || $1::text
