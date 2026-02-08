-- Expressions in INSERT VALUES
CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER);

-- String concatenation in VALUES
INSERT INTO t1 VALUES (1, 'Alice' || ' Bob', 10 + 20);
SELECT * FROM t1;

-- Function calls in VALUES
INSERT INTO t1 VALUES (2, UPPER('hello'), ABS(-42));
SELECT * FROM t1 WHERE id = 2;

-- Arithmetic expressions
INSERT INTO t1 VALUES (3, 'calc', 100 / 4 + 5 * 3);
SELECT * FROM t1 WHERE id = 3;

-- Multi-row with expressions
INSERT INTO t1 VALUES (4, 'a' || 'b', 1 + 1), (5, LOWER('XYZ'), 2 * 3);
SELECT * FROM t1 WHERE id >= 4 ORDER BY id;

-- NULL in expressions
INSERT INTO t1 VALUES (6, NULL, COALESCE(NULL, 99));
SELECT * FROM t1 WHERE id = 6;

-- Negative values
INSERT INTO t1 VALUES (7, 'neg', -10);
SELECT * FROM t1 WHERE id = 7;

-- IIF in VALUES
INSERT INTO t1 VALUES (8, IIF(1 > 0, 'yes', 'no'), IIF(1 > 0, 100, 200));
SELECT * FROM t1 WHERE id = 8;
