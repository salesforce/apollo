-- file:updatable_views.sql ln:915 expect:true
CREATE VIEW rw_view2 WITH (security_barrier = true) AS
  SELECT * FROM rw_view1 WHERE snoop(person)
