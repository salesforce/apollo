-- file:jsonb.sql ln:631 expect:true
SELECT reca FROM jsonb_populate_record(NULL::jsbrec, '{"reca": "{\"(abc,42,01.02.2003)\"}"}') q
