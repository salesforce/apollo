-- file:plpgsql.sql ln:3841 expect:true
create function error1(text) returns text language sql as
$$ SELECT relname::text FROM pg_class c WHERE c.oid = $1::regclass $$
