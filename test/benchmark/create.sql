SET client_min_messages TO error;

DROP SCHEMA tablelog_benchmark CASCADE;
CREATE SCHEMA tablelog_benchmark;
SET search_path TO tablelog_benchmark,public;

CREATE TABLE test (
    i integer NOT NULL PRIMARY KEY
);
SELECT table_log_init(4,'test');

CREATE TABLE time (
    time timestamptz NOT NULL
);

