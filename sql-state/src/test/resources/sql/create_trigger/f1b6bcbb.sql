-- file:triggers.sql ln:691 expect:true
CREATE TRIGGER invalid_trig AFTER INSERT ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_ins_row')
