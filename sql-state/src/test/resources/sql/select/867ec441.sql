-- file:jsonb.sql ln:137 expect:true
SELECT test_json ->> 6 FROM test_jsonb WHERE json_type = 'array'
