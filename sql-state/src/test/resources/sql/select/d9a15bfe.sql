-- file:json.sql ln:500 expect:true
SELECT jsa FROM json_populate_record(NULL::jsrec, '{"jsa": ["aaa", null, [1, 2, "3", {}], { "k" : "v" }]}') q
