-- Aggregates with expressions
CREATE TABLE t (id INTEGER, category TEXT, value INTEGER);
INSERT INTO t VALUES (1, 'A', 10);
INSERT INTO t VALUES (2, 'A', 20);
INSERT INTO t VALUES (3, 'B', 30);
INSERT INTO t VALUES (4, 'B', 40);
INSERT INTO t VALUES (5, 'A', 50);

-- SUM with GROUP BY
SELECT category, SUM(value) AS total FROM t GROUP BY category ORDER BY category;

-- COUNT with HAVING
SELECT category, COUNT(*) AS cnt FROM t GROUP BY category HAVING COUNT(*) > 2 ORDER BY category;

-- Multiple aggregates
SELECT category, MIN(value), MAX(value), SUM(value), COUNT(*) FROM t GROUP BY category ORDER BY category;

-- AVG
SELECT category, AVG(value) FROM t GROUP BY category ORDER BY category;
