-- file:json.sql ln:332 expect:true
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::json#>>array['f2','1']
