-- file:txid.sql ln:25 expect:true
select  txid_snapshot_xmin(snap),
	txid_snapshot_xmax(snap),
	txid_snapshot_xip(snap)
from snapshot_test order by nr
