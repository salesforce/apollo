-- file:updatable_views.sql ln:228 expect:true
CREATE RULE rw_view1_del_rule AS ON DELETE TO rw_view1
  DO INSTEAD DELETE FROM base_tbl WHERE a=OLD.a RETURNING OLD.*
