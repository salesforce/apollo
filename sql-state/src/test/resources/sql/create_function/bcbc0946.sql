-- file:rangetypes.sql ln:496 expect:true
create function outparam_fail(i anyelement, out r anyrange, out t text)
  as $$ select '[1,10]', 'foo' $$ language sql
