-- file:create_index.sql ln:1067 expect:true
SELECT  b.relname,
        b.relkind,
        CASE WHEN a.relfilenode = b.relfilenode THEN 'relfilenode is unchanged'
        ELSE 'relfilenode has changed' END
  FROM reindex_before b JOIN pg_class a ON b.oid = a.oid
  ORDER BY 1
