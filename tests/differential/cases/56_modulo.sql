CREATE TABLE t (id INTEGER, val INTEGER);
INSERT INTO t VALUES (1, 10), (2, 7), (3, 15), (4, 20);

SELECT val, val % 3 FROM t;

SELECT * FROM t WHERE val % 2 = 0;

SELECT val % 5 FROM t;
