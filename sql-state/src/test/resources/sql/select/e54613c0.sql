-- file:strings.sql ln:489 expect:true
select md5('abcdefghijklmnopqrstuvwxyz'::bytea) = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE"
