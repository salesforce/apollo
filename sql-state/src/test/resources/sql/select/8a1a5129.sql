-- file:jsonb.sql ln:432 expect:true
SELECT jsonb_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text)
