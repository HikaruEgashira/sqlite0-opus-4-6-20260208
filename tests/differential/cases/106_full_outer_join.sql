-- FULL OUTER JOIN
CREATE TABLE t1 (id INTEGER, name TEXT);
CREATE TABLE t2 (id INTEGER, dept TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, 'bob');
INSERT INTO t1 VALUES (3, 'carol');
INSERT INTO t2 VALUES (2, 'eng');
INSERT INTO t2 VALUES (3, 'sales');
INSERT INTO t2 VALUES (4, 'hr');

-- FULL OUTER JOIN shows all rows from both tables
SELECT t1.name, t2.dept FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id, t2.id;

-- FULL JOIN (without OUTER keyword)
SELECT t1.name, t2.dept FROM t1 FULL JOIN t2 ON t1.id = t2.id ORDER BY t1.id, t2.id;
