-- file:rangefuncs.sql ln:318 expect:true
SELECT * FROM (VALUES (1),(2),(3)) v1(r1),
              LATERAL (SELECT r1, * FROM (VALUES (10),(20),(30)) v2(r2)
                                         LEFT JOIN generate_series(r1,2+r2/5) f(i) ON ((r2+i)<100) OFFSET 0) s1
