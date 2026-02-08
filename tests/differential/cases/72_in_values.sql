CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER);
INSERT INTO t1 VALUES (1, 'alice', 10);
INSERT INTO t1 VALUES (2, 'bob', 20);
INSERT INTO t1 VALUES (3, 'carol', 30);
INSERT INTO t1 VALUES (4, 'dave', 40);

-- IN with integer list
SELECT * FROM t1 WHERE id IN (1, 3) ORDER BY id;

-- IN with string list
SELECT * FROM t1 WHERE name IN ('alice', 'carol') ORDER BY id;

-- NOT IN with value list
SELECT * FROM t1 WHERE id NOT IN (1, 2) ORDER BY id;

-- IN with expression
SELECT * FROM t1 WHERE val IN (10, 20, 30) ORDER BY id;

-- IN with single value
SELECT * FROM t1 WHERE id IN (2) ORDER BY id;

-- IN combined with other conditions
SELECT * FROM t1 WHERE id IN (1, 2, 3) AND val > 15 ORDER BY id;
