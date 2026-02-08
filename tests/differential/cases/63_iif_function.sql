CREATE TABLE t1 (id INTEGER, val INTEGER);
INSERT INTO t1 VALUES (1, 10);
INSERT INTO t1 VALUES (2, -5);
INSERT INTO t1 VALUES (3, 0);

-- IIF with true condition
SELECT IIF(1, 'yes', 'no');

-- IIF with false condition
SELECT IIF(0, 'yes', 'no');

-- IIF with column reference
SELECT id, IIF(val > 0, 'positive', 'non-positive') FROM t1 ORDER BY id;

-- IIF with NULL condition
SELECT IIF(NULL, 'yes', 'no');

-- Nested IIF
SELECT IIF(val > 0, 'pos', IIF(val < 0, 'neg', 'zero')) FROM t1 ORDER BY id;
