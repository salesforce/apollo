-- file:jsonb.sql ln:549 expect:true
SELECT i FROM jsonb_populate_record(NULL::jsbrec_i_not_null, '{"i": 12345}') q
