-- file:json.sql ln:510 expect:true
SELECT reca FROM json_populate_record(NULL::jsrec, '{"reca": ["(abc,42,01.02.2003)"]}') q
