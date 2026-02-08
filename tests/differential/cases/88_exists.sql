-- EXISTS and NOT EXISTS tests
CREATE TABLE t1 (id INTEGER, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, 'bob');
CREATE TABLE t2 (id INTEGER, t1_id INTEGER);
INSERT INTO t2 VALUES (1, 1);
INSERT INTO t2 VALUES (2, 1);

-- EXISTS returns 1 when subquery has rows
SELECT EXISTS(SELECT 1 FROM t1);

-- NOT EXISTS returns 0 when subquery has rows
SELECT NOT EXISTS(SELECT 1 FROM t1);

-- EXISTS returns 0 for empty result
SELECT EXISTS(SELECT 1 FROM t1 WHERE id > 100);

-- NOT EXISTS returns 1 for empty result
SELECT NOT EXISTS(SELECT 1 FROM t1 WHERE id > 100);
