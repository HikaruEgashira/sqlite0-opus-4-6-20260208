-- Columns without explicit type
CREATE TABLE t1 (a, b, c);
INSERT INTO t1 VALUES (1, 'hello', 3.14);
INSERT INTO t1 VALUES (2, 'world', 2.71);
SELECT * FROM t1;

-- Mixed: some typed, some untyped
CREATE TABLE t2 (id INTEGER PRIMARY KEY, name, value REAL);
INSERT INTO t2 VALUES (1, 'Alice', 10.5);
INSERT INTO t2 VALUES (2, 'Bob', 20.5);
SELECT * FROM t2;

-- Aggregates on untyped columns
SELECT SUM(a), SUM(c) FROM t1;

-- Untyped column with constraints
CREATE TABLE t3 (id PRIMARY KEY, name NOT NULL);
INSERT INTO t3 VALUES (1, 'test');
SELECT * FROM t3;
