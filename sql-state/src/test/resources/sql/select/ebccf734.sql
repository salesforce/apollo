-- file:jsonb.sql ln:941 expect:true
select pg_column_size('{}'::jsonb || '{"aa":1, "b":2}'::jsonb) = pg_column_size('{"aa":1, "b":2}'::jsonb)
