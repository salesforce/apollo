-- file:jsonb.sql ln:321 expect:true
SELECT jsonb_build_array(VARIADIC '{1,2,3,4}'::text[])
