-- file:aggregates.sql ln:423 expect:true
select aggfns(distinct a,b,c order by a,c using ~<~,b)
  from (values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz')) v(a,b,c),
       generate_series(1,2) i
