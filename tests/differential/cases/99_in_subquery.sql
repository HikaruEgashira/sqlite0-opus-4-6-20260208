-- IN with value lists and subqueries
CREATE TABLE departments (id INTEGER, name TEXT);
INSERT INTO departments VALUES (1, 'eng');
INSERT INTO departments VALUES (2, 'sales');
INSERT INTO departments VALUES (3, 'hr');

CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER);
INSERT INTO employees VALUES (1, 'alice', 1);
INSERT INTO employees VALUES (2, 'bob', 2);
INSERT INTO employees VALUES (3, 'carol', 1);
INSERT INTO employees VALUES (4, 'dave', 3);

-- IN value list
SELECT name FROM employees WHERE dept_id IN (1, 2) ORDER BY name;

-- NOT IN value list
SELECT name FROM employees WHERE dept_id NOT IN (1) ORDER BY name;

-- IN subquery
SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'eng') ORDER BY name;

-- NOT IN subquery
SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'hr') ORDER BY name;
