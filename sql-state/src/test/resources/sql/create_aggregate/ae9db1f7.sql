-- file:create_aggregate.sql ln:203 expect:true
CREATE AGGREGATE wrongreturntype (float8)
(
    stype = float8,
    sfunc = float8pl,
    mstype = float8,
    msfunc = float8pl,
    minvfunc = float8mi_int
)
