-- file:functional_deps.sql ln:109 expect:true
CREATE TEMP TABLE node (
    nid SERIAL,
    vid integer NOT NULL default '0',
    type varchar(32) NOT NULL default '',
    title varchar(128) NOT NULL default '',
    uid integer NOT NULL default '0',
    status integer NOT NULL default '1',
    created integer NOT NULL default '0',
        PRIMARY KEY (nid, vid)
)
