-- file:tstypes.sql ln:211 expect:true
SELECT ts_delete('base:7 hidden:6 rebel:1 spaceship:2,33A,34B,35C,36D strike:3'::tsvector, ARRAY['spaceship','leya','rebel'])
