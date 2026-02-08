CREATE TABLE t1 (id INTEGER, dept TEXT, name TEXT);
INSERT INTO t1 VALUES (1, 'eng', 'alice');
INSERT INTO t1 VALUES (2, 'eng', 'bob');
INSERT INTO t1 VALUES (3, 'sales', 'carol');
INSERT INTO t1 VALUES (4, 'sales', 'dave');
INSERT INTO t1 VALUES (5, 'eng', 'eve');

-- GROUP_CONCAT with GROUP BY
SELECT dept, GROUP_CONCAT(name) FROM t1 GROUP BY dept;

-- GROUP_CONCAT with custom separator
SELECT dept, GROUP_CONCAT(name, ' | ') FROM t1 GROUP BY dept;

-- GROUP_CONCAT without GROUP BY (all rows)
SELECT GROUP_CONCAT(name) FROM t1;

-- GROUP_CONCAT with NULL values
INSERT INTO t1 VALUES (6, 'eng', NULL);
SELECT dept, GROUP_CONCAT(name) FROM t1 GROUP BY dept;
