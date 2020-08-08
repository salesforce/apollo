-- file:opr_sanity.sql ln:881 expect:true
SELECT ctid, aggfnoid::oid
FROM pg_aggregate as p1
WHERE aggmtranstype = 0 AND
    (aggmtransfn != 0 OR aggminvtransfn != 0 OR aggmfinalfn != 0 OR
     aggmtransspace != 0 OR aggminitval IS NOT NULL)
