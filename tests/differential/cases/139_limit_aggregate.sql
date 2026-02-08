-- LIMIT/OFFSET with aggregates
CREATE TABLE t (id INTEGER, category TEXT, value INTEGER);
INSERT INTO t VALUES (1, 'A', 10);
INSERT INTO t VALUES (2, 'B', 20);
INSERT INTO t VALUES (3, 'C', 30);
INSERT INTO t VALUES (4, 'A', 40);
INSERT INTO t VALUES (5, 'B', 50);

-- GROUP BY with LIMIT
SELECT category, SUM(value) AS total FROM t GROUP BY category ORDER BY total DESC LIMIT 2;

-- GROUP BY with OFFSET
SELECT category, SUM(value) AS total FROM t GROUP BY category ORDER BY total DESC LIMIT 1 OFFSET 1;
