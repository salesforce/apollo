-- file:brin.sql ln:456 expect:true
CREATE INDEX brin_test_b_idx ON brin_test USING brin (b) WITH (pages_per_range = 2)
