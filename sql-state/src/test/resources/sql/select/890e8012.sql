-- file:jsonb.sql ln:875 expect:true
SELECT '[0,1,2,[3,4],{"5":"five"}]'::jsonb #> '{4,5}'
