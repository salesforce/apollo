-- file:jsonb.sql ln:281 expect:true
SELECT jsonb '{"a":null, "b":"qq"}' ?| ARRAY['c','d']
