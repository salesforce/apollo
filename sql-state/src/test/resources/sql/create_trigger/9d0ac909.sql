-- file:updatable_views.sql ln:300 expect:true
CREATE TRIGGER rw_view1_ins_trig INSTEAD OF INSERT ON rw_view1
  FOR EACH ROW EXECUTE PROCEDURE rw_view1_trig_fn()
