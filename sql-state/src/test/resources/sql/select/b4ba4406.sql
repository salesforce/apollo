-- file:stats.sql ln:171 expect:true
SELECT pr.snap_ts < pg_stat_get_snapshot_timestamp() as snapshot_newer
FROM prevstats AS pr
