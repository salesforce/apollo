-- file:plpgsql.sql ln:3097 expect:true
create temp table forc_test as
  select n as i, n as j from generate_series(1,10) n
