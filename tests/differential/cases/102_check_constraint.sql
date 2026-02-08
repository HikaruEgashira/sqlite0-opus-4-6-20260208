-- CHECK constraint tests (valid values only)
CREATE TABLE t1 (id INTEGER, score INTEGER CHECK(score >= 0));
INSERT INTO t1 VALUES (1, 50);
INSERT INTO t1 VALUES (2, 75);
INSERT INTO t1 VALUES (3, 0);
SELECT * FROM t1 ORDER BY id;
