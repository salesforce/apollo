-- file:aggregates.sql ln:459 expect:true
create or replace view agg_view1 as
  select aggfns(a,a,c order by b)
    from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c)
