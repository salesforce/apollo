-- file:btree_index.sql ln:34 expect:true
SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::text
