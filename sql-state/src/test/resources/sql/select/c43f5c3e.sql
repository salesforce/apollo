-- file:jsonb.sql ln:123 expect:true
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar'
