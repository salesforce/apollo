-- file:updatable_views.sql ln:501 expect:true
CREATE TRIGGER rw_view1_ins_trig AFTER INSERT ON base_tbl
  FOR EACH ROW EXECUTE PROCEDURE rw_view1_trig_fn()
