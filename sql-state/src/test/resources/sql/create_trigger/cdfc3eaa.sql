-- file:updatable_views.sql ln:810 expect:true
CREATE TRIGGER rw_view1_trig
  INSTEAD OF INSERT OR UPDATE OR DELETE ON rw_view1
  FOR EACH ROW EXECUTE PROCEDURE rw_view1_trig_fn()
