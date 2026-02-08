-- GROUP BY with expressions
CREATE TABLE t (id INTEGER, name TEXT, value INTEGER);
INSERT INTO t VALUES (1, 'Alice', 10);
INSERT INTO t VALUES (2, 'Bob', 20);
INSERT INTO t VALUES (3, 'alice', 30);
INSERT INTO t VALUES (4, 'Bob', 40);
INSERT INTO t VALUES (5, 'ALICE', 50);

-- GROUP BY with function
SELECT UPPER(name) AS uname, SUM(value) FROM t GROUP BY UPPER(name) ORDER BY uname;

-- GROUP BY with expression
SELECT id % 2 AS parity, COUNT(*) FROM t GROUP BY id % 2 ORDER BY parity;

-- GROUP BY with LOWER
SELECT LOWER(name) AS lname, COUNT(*) FROM t GROUP BY LOWER(name) ORDER BY lname;
