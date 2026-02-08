CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER);
INSERT INTO t1 VALUES (1, 'alice', 10);
INSERT INTO t1 VALUES (2, 'bob', 20);
INSERT INTO t1 VALUES (3, 'carol', 30);

-- Column alias with AS (output values should be the same)
SELECT id AS user_id, name AS user_name FROM t1;

-- Expression alias
SELECT val * 2 AS doubled FROM t1;

-- Alias with aggregate
SELECT COUNT(*) AS total FROM t1;
SELECT SUM(val) AS total_val FROM t1;
