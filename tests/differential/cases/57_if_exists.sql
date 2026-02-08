CREATE TABLE t1 (id INTEGER, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');

-- IF NOT EXISTS: should not error or overwrite
CREATE TABLE IF NOT EXISTS t1 (id INTEGER, name TEXT);
SELECT * FROM t1;

-- IF EXISTS: should not error on missing table
DROP TABLE IF EXISTS nonexistent;
SELECT * FROM t1;

-- IF EXISTS: should actually drop
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (id INTEGER);
INSERT INTO t1 VALUES (42);
SELECT * FROM t1;
