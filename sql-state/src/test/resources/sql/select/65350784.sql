-- file:hash_index.sql ln:79 expect:true
SELECT h.seqno AS i1492, h.random AS i1
   FROM hash_i4_heap h
   WHERE h.random = 1
