-- file:updatable_views.sql ln:951 expect:true
CREATE RULE base_tbl_del_rule AS ON DELETE TO base_tbl
  DO INSTEAD
    UPDATE base_tbl SET deleted = true WHERE id = old.id
