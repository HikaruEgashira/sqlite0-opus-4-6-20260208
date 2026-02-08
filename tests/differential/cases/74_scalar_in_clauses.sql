CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER);
INSERT INTO t1 VALUES (1, 'Alice', -10);
INSERT INTO t1 VALUES (2, 'bob', 20);
INSERT INTO t1 VALUES (3, 'CAROL', -30);
INSERT INTO t1 VALUES (4, 'dave', 40);

-- Scalar function in WHERE
SELECT * FROM t1 WHERE ABS(val) > 15 ORDER BY id;

-- UPPER/LOWER in WHERE
SELECT * FROM t1 WHERE LOWER(name) = 'alice';

-- LENGTH in WHERE
SELECT * FROM t1 WHERE LENGTH(name) > 3 ORDER BY id;

-- SUBSTR in SELECT + WHERE
SELECT id, SUBSTR(name, 1, 3) FROM t1 WHERE id IN (1, 2, 3) ORDER BY id;

-- Scalar function with arithmetic
SELECT id, ABS(val) * 2 FROM t1 ORDER BY id;

-- IIF in SELECT
SELECT id, IIF(val > 0, 'pos', 'neg') FROM t1 ORDER BY id;
