-- file:jsonb.sql ln:323 expect:true
SELECT jsonb_build_array(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][])
