-- file:plpgsql.sql ln:4716 expect:true
DELETE FROM transition_table_level1
  WHERE level1_no BETWEEN 100000000 AND 100000010
