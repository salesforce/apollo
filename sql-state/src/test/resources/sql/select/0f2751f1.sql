-- file:line.sql ln:76 expect:true
SELECT line '[(0,0),(1,1)]' ?-| line '[(1,0),(1,1)]'
