-- file:create_index.sql ln:609 expect:true
SELECT * FROM array_index_op_test WHERE i @> '{}' ORDER BY seqno
