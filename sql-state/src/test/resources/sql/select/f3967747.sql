-- file:btree_index.sql ln:30 expect:true
SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::text
