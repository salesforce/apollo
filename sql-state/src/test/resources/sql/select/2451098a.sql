-- file:jsonb.sql ln:343 expect:true
SELECT jsonb_build_object(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][])
