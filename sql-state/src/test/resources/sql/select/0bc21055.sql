-- file:jsonb.sql ln:214 expect:true
SELECT jsonb_contained('{"a":"b", "c":null}', '{"a":"b", "b":1, "c":null}')
