-- file:hash_index.sql ln:106 expect:true
SELECT h.seqno AS emptyset
   FROM hash_name_heap h
   WHERE h.random = '76652222'::name
