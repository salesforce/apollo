-- file:plpgsql.sql ln:4603 expect:false
BEGIN
    WITH p AS (SELECT level1_no, sum(delta) cnt
                 FROM (SELECT level1_no, 1 AS delta FROM i
                       UNION ALL
                       SELECT level1_no, -1 AS delta FROM d) w
                 GROUP BY level1_no
                 HAVING sum(delta) < 0)
    SELECT level1_no
      FROM p JOIN transition_table_level2 c ON c.parent_no = p.level1_no
      INTO x
