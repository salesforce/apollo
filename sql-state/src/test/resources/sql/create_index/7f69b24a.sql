-- file:brin.sql ln:427 expect:true
CREATE INDEX brin_summarize_idx ON brin_summarize USING brin (value) WITH (pages_per_range=2)
