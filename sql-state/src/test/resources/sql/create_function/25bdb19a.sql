-- file:rangetypes.sql ln:407 expect:true
create function bogus_func(anyelement)
  returns anyrange as 'select int4range(1,10)' language sql
