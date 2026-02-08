-- Table-qualified column references
CREATE TABLE t1 (id INTEGER, val TEXT);
CREATE TABLE t2 (id INTEGER, val TEXT);
INSERT INTO t1 VALUES (1, 'a');
INSERT INTO t1 VALUES (2, 'b');
INSERT INTO t2 VALUES (1, 'x');
INSERT INTO t2 VALUES (3, 'y');
SELECT t1.id, t1.val, t2.val FROM t1 INNER JOIN t2 ON t1.id = t2.id;
