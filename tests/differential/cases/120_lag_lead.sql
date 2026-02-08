-- LAG and LEAD window functions
CREATE TABLE t (id INTEGER, val INTEGER);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);
INSERT INTO t VALUES (4, 40);
INSERT INTO t VALUES (5, 50);

-- LAG(col) - default offset 1
SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val FROM t ORDER BY id;

-- LAG with offset 2
SELECT id, val, LAG(val, 2) OVER (ORDER BY id) AS prev2 FROM t ORDER BY id;

-- LAG with default value
SELECT id, val, LAG(val, 1, 0) OVER (ORDER BY id) AS prev_val FROM t ORDER BY id;

-- LEAD(col)
SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next_val FROM t ORDER BY id;

-- LEAD with offset 2 and default
SELECT id, val, LEAD(val, 2, -1) OVER (ORDER BY id) AS next2 FROM t ORDER BY id;
