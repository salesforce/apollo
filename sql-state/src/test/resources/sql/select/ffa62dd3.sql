-- file:tstypes.sql ln:216 expect:true
SELECT unnest('base:7 hidden:6 rebel:1 spaceship:2,33A,34B,35C,36D strike:3'::tsvector)
