CREATE TABLE t1 (id INTEGER, dept TEXT, salary INTEGER);
INSERT INTO t1 VALUES (1, 'eng', 100);
INSERT INTO t1 VALUES (2, 'eng', 200);
INSERT INTO t1 VALUES (3, 'sales', 150);
INSERT INTO t1 VALUES (4, 'sales', 100);
INSERT INTO t1 VALUES (5, 'eng', 150);

-- Single ORDER BY (baseline)
SELECT * FROM t1 ORDER BY dept;

-- Multi ORDER BY: dept ASC, salary DESC
SELECT * FROM t1 ORDER BY dept ASC, salary DESC;

-- Multi ORDER BY: dept ASC, salary ASC
SELECT * FROM t1 ORDER BY dept, salary;

-- Multi ORDER BY: salary DESC, dept ASC
SELECT * FROM t1 ORDER BY salary DESC, dept;
