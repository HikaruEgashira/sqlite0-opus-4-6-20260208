-- UPDATE with complex WHERE expressions
CREATE TABLE t1 (id INTEGER, name TEXT, score INTEGER);
INSERT INTO t1 VALUES (1, 'alice', 80);
INSERT INTO t1 VALUES (2, 'bob', 60);
INSERT INTO t1 VALUES (3, 'carol', 90);
INSERT INTO t1 VALUES (4, 'dave', 70);

-- UPDATE with AND/OR
UPDATE t1 SET score = 100 WHERE name = 'alice' OR name = 'carol';
SELECT * FROM t1 ORDER BY id;

-- UPDATE with expression in SET
UPDATE t1 SET score = score + 10 WHERE score < 80;
SELECT * FROM t1 ORDER BY id;

-- UPDATE with function in WHERE
UPDATE t1 SET name = UPPER(name) WHERE LENGTH(name) = 3;
SELECT * FROM t1 ORDER BY id;
