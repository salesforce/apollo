-- file:json.sql ln:727 expect:true
select json_strip_nulls('{"a":1,"b":null,"c":[2,null,3],"d":{"e":4,"f":null}}')
