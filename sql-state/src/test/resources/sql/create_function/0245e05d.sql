-- file:polymorphism.sql ln:751 expect:true
create function dfunc(a anyelement, b anyelement = null, flag bool = true)
returns anyelement as $$
  select case when $3 then $1 else $2 end
