-- LIKE pattern matching comprehensive test
CREATE TABLE t1 (name TEXT);
INSERT INTO t1 VALUES ('alice');
INSERT INTO t1 VALUES ('Alice');
INSERT INTO t1 VALUES ('bob');
INSERT INTO t1 VALUES ('ALICE');

-- LIKE is case-insensitive in SQLite
SELECT name FROM t1 WHERE name LIKE 'alice';
SELECT name FROM t1 WHERE name LIKE 'A%';
SELECT name FROM t1 WHERE name LIKE '%ice';
SELECT name FROM t1 WHERE name LIKE '_lice';
SELECT name FROM t1 WHERE name LIKE '%LI%';
