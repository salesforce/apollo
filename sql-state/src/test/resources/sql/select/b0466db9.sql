-- file:plpgsql.sql ln:4277 expect:true
select consumes_rw_array(a), a from
  (values (returns_rw_array(1)), (returns_rw_array(2))) v(a)
