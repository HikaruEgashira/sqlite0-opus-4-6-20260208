-- Aggregate expressions in SELECT and ORDER BY
CREATE TABLE t (cat TEXT, val INTEGER);
INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5);

-- Arithmetic with aggregates
SELECT cat, SUM(val) * 2 AS doubled FROM t GROUP BY cat ORDER BY 1;

-- ORDER BY aggregate expression
SELECT cat, SUM(val) * 2 AS doubled FROM t GROUP BY cat ORDER BY SUM(val) DESC;

-- Multiple aggregate expressions
SELECT cat, COUNT(*) || ' items' AS cnt, SUM(val) + 100 AS total_plus FROM t GROUP BY cat ORDER BY 1;

-- Aggregate in CASE expression
SELECT cat, CASE WHEN SUM(val) > 5 THEN 'big' ELSE 'small' END AS size FROM t GROUP BY cat ORDER BY 1;

-- Nested function with aggregate
SELECT cat, ROUND(AVG(val), 1) AS avg_r FROM t GROUP BY cat ORDER BY 1;

-- ABS of aggregate
SELECT cat, ABS(SUM(val) - 5) AS diff_from_5 FROM t GROUP BY cat ORDER BY 1;
