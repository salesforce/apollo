-- file:jsonb.sql ln:849 expect:true
SELECT '["a","b","c",[1,2],null]'::jsonb -> 0
