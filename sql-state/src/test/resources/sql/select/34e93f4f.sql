-- file:json.sql ln:251 expect:true
select (test_json->3) is null as expect_false
from test_json
where json_type = 'array'
