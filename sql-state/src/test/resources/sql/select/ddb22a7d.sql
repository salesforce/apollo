-- file:jsonb.sql ln:816 expect:true
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4},3]}'
