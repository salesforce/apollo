-- file:plpgsql.sql ln:4671 expect:true
INSERT INTO transition_table_level1(level1_no)
  SELECT generate_series(201,1000)
