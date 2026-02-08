-- GROUP BY column position number
CREATE TABLE t1 (name TEXT, category TEXT, value INTEGER);
INSERT INTO t1 VALUES ('Alice', 'A', 10), ('Bob', 'B', 20), ('Carol', 'A', 30), ('Dave', 'B', 40);

-- GROUP BY 2nd column (category) - position refers to SELECT column
SELECT category, SUM(value) FROM t1 GROUP BY 1;

-- GROUP BY 1st column (name)
SELECT name, SUM(value) FROM t1 GROUP BY 1;

-- GROUP BY position with ORDER BY position
SELECT category, COUNT(*), SUM(value) FROM t1 GROUP BY 1 ORDER BY 3 DESC;

-- GROUP BY multiple positions
CREATE TABLE t2 (a TEXT, b TEXT, c INTEGER);
INSERT INTO t2 VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3), ('y', 'q', 4);
SELECT a, b, SUM(c) FROM t2 GROUP BY 1, 2;

-- GROUP BY position with HAVING
SELECT category, SUM(value) AS total FROM t1 GROUP BY 1 HAVING SUM(value) > 30;
