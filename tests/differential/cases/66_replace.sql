CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);
INSERT INTO t1 VALUES (1, 'alice', 10);
INSERT INTO t1 VALUES (2, 'bob', 20);
INSERT INTO t1 VALUES (3, 'carol', 30);

-- REPLACE with existing PK (should update)
REPLACE INTO t1 VALUES (2, 'dave', 25);
SELECT * FROM t1 ORDER BY id;

-- REPLACE with new PK (should insert)
REPLACE INTO t1 VALUES (4, 'eve', 40);
SELECT * FROM t1 ORDER BY id;

-- REPLACE with another existing PK
REPLACE INTO t1 VALUES (1, 'frank', 15);
SELECT * FROM t1 ORDER BY id;
