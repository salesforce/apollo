-- file:json.sql ln:203 expect:true
SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array'
