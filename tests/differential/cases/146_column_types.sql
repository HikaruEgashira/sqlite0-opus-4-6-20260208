-- Extended column type support (type affinity mapping)
CREATE TABLE t1 (id INT, name VARCHAR(100), active BOOLEAN, price DECIMAL(10,2));
INSERT INTO t1 VALUES (1, 'Alice', 1, 99);
INSERT INTO t1 VALUES (2, 'Bob', 0, 150);
SELECT * FROM t1 ORDER BY id;

CREATE TABLE t2 (a TINYINT, b SMALLINT, c BIGINT, d FLOAT, e DOUBLE);
INSERT INTO t2 VALUES (1, 2, 3, 4.5, 6.7);
SELECT * FROM t2;

CREATE TABLE t3 (data BLOB, notes CLOB, code CHAR(10));
INSERT INTO t3 VALUES ('binary', 'long text', 'ABC');
SELECT * FROM t3;
