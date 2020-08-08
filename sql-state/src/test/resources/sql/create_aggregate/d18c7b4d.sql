-- file:create_aggregate.sql ln:129 expect:true
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	serialfunc = numeric_avg_deserialize,
	deserialfunc = numeric_avg_deserialize
)
