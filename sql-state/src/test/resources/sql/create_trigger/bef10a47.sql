-- file:object_address.sql ln:33 expect:true
CREATE TRIGGER t BEFORE INSERT ON addr_nsp.gentable FOR EACH ROW EXECUTE PROCEDURE addr_nsp.trig()
