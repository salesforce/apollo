-- file:create_index.sql ln:739 expect:true
create unique index hash_f8_index_2 on hash_f8_heap((seqno + 1), random)
