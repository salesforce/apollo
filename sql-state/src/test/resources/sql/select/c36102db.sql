-- file:aggregates.sql ln:62 expect:true
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest
