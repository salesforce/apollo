-- file:rangetypes.sql ln:480 expect:true
create function outparam_succeed(i anyrange, out r anyrange, out t text)
  as $$ select $1, 'foo'::text $$ language sql
