-- file:json.sql ln:421 expect:true
select * from json_populate_record(null::jpop,'{"a":[100,200,false],"x":43.2}') q
