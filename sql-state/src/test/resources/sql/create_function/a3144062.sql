-- file:rangetypes.sql ln:504 expect:true
create function table_fail(i anyelement) returns table(i anyelement, r anyrange)
  as $$ select $1, '[1,10]' $$ language sql
