-- file:json.sql ln:211 expect:true
SELECT test_json->>2
FROM test_json
WHERE json_type = 'array'
