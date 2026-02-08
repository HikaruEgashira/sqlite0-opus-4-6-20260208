-- Float literal tests
SELECT 3.14;
SELECT -2.5;
SELECT 0.5 + 0.5;
SELECT 3.14 * 2;

-- REAL column type
CREATE TABLE t1 (id INTEGER, val REAL);
INSERT INTO t1 VALUES (1, 3.14);
INSERT INTO t1 VALUES (2, -1.5);
INSERT INTO t1 VALUES (3, 0.0);
SELECT * FROM t1 ORDER BY id;

-- Float comparisons
SELECT val FROM t1 WHERE val > 0.0 ORDER BY val;
