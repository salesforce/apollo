-- file:jsonb.sql ln:445 expect:true
SELECT '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>array['f4','f6']
