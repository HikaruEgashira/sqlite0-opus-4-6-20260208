-- ORDER BY column position number
CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);
INSERT INTO t1 VALUES (1, 'Charlie', 30), (2, 'Alice', 10), (3, 'Bob', 20);

-- ORDER BY 1st column (id)
SELECT * FROM t1 ORDER BY 1;

-- ORDER BY 2nd column (name)
SELECT * FROM t1 ORDER BY 2;

-- ORDER BY 3rd column DESC
SELECT * FROM t1 ORDER BY 3 DESC;

-- ORDER BY with multiple positions
SELECT * FROM t1 ORDER BY 3, 2;

-- ORDER BY position in aggregate query
SELECT name, SUM(value) AS total FROM t1 GROUP BY name ORDER BY 2 DESC;

-- ORDER BY position with expressions
SELECT id, name, value * 2 AS doubled FROM t1 ORDER BY 3 DESC;

-- ORDER BY position with JOIN
CREATE TABLE t2 (id INTEGER, category TEXT);
INSERT INTO t2 VALUES (1, 'A'), (2, 'B'), (3, 'A');
SELECT t1.name, t2.category FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY 2, 1;
