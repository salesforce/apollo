-- file:triggers.sql ln:924 expect:true
CREATE TRIGGER no_op_trig INSTEAD OF INSERT OR UPDATE OR DELETE
ON european_city_view FOR EACH ROW EXECUTE PROCEDURE no_op_trig_fn()
