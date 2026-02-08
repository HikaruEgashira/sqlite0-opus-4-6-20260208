CREATE TABLE t1 (id INTEGER, name TEXT, val INTEGER);
INSERT INTO t1 VALUES (1, 'alice', 10);
INSERT INTO t1 VALUES (2, 'bob', 20);
INSERT INTO t1 VALUES (3, 'carol', 30);

-- UPDATE with arithmetic expression
UPDATE t1 SET val = val + 5 WHERE id = 1;
SELECT * FROM t1 ORDER BY id;

-- UPDATE with expression involving multiple columns
UPDATE t1 SET val = val * 2 WHERE id = 2;
SELECT * FROM t1 ORDER BY id;

-- UPDATE with expression, no WHERE (all rows)
UPDATE t1 SET val = val - 1;
SELECT * FROM t1 ORDER BY id;

-- UPDATE with string literal (still works)
UPDATE t1 SET name = 'dave' WHERE id = 3;
SELECT * FROM t1 ORDER BY id;

-- UPDATE multiple columns with expressions
UPDATE t1 SET name = 'eve', val = val + 100 WHERE id = 1;
SELECT * FROM t1 ORDER BY id;
