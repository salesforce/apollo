-- file:hash_index.sql ln:126 expect:true
UPDATE hash_f8_heap
   SET random = '-1234.1234'::float8
   WHERE hash_f8_heap.seqno = 8906
