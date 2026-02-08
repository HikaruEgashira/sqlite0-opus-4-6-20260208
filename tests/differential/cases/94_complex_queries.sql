-- Complex query patterns
CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER);
INSERT INTO employees VALUES (1, 'alice', 'eng', 100);
INSERT INTO employees VALUES (2, 'bob', 'eng', 120);
INSERT INTO employees VALUES (3, 'carol', 'sales', 80);
INSERT INTO employees VALUES (4, 'dave', 'sales', 90);
INSERT INTO employees VALUES (5, 'eve', 'eng', 110);

-- Subquery in WHERE with aggregate
SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);

-- Multiple conditions with functions
SELECT name FROM employees WHERE LENGTH(name) > 3 AND dept = 'eng' ORDER BY name;

-- Nested IFNULL and COALESCE
SELECT IFNULL(NULL, COALESCE(NULL, 'final'));
