-- file:line.sql ln:56 expect:true
SELECT lseg '[(1,1),(2,2)]' <@ line '[(0,0),(2,2)]'
