-- file:jsonb.sql ln:863 expect:true
SELECT '{"a":"b","c":[1,2,3]}'::jsonb #> '{c}'
