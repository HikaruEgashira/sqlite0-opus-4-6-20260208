-- Scalar subqueries (non-correlated)
CREATE TABLE t (id INTEGER, val INTEGER);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);

-- Subquery in SELECT
SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t ORDER BY id;

-- Subquery in WHERE
SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id;

-- Subquery comparison
SELECT * FROM t WHERE val = (SELECT MIN(val) FROM t);
