-- file:jsonb.sql ln:287 expect:true
SELECT jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['c','d'])
