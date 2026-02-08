-- <> operator as alias for !=
CREATE TABLE t1 (id INTEGER, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, 'bob');
INSERT INTO t1 VALUES (3, 'carol');

SELECT * FROM t1 WHERE id <> 2;
SELECT * FROM t1 WHERE name <> 'bob';
SELECT * FROM t1 WHERE id <> 1 AND id <> 3;
