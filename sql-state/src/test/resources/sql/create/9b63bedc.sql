-- file:polymorphism.sql ln:628 expect:true
create or replace function dfunc(a variadic int[] default array[]::int[]) returns int as
$$ select array_upper($1, 1) $$ language sql
