-- file:jsonb.sql ln:388 expect:true
SELECT jsonb_object('{a,1,b,2,3,NULL,"d e f","a b c"}')
