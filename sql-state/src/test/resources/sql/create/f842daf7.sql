-- file:matview.sql ln:33 expect:true
CREATE UNIQUE INDEX mvtest_tvmm_pred ON mvtest_tvmm (grandtot) WHERE grandtot < 0
