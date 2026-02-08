-- NATURAL JOIN
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER);
CREATE TABLE departments (dept_id INTEGER, dept_name TEXT);
INSERT INTO employees VALUES (1, 'alice', 10);
INSERT INTO employees VALUES (2, 'bob', 20);
INSERT INTO employees VALUES (3, 'carol', 10);
INSERT INTO departments VALUES (10, 'engineering');
INSERT INTO departments VALUES (20, 'sales');
INSERT INTO departments VALUES (30, 'hr');

-- NATURAL JOIN matches on dept_id
SELECT name, dept_name FROM employees NATURAL JOIN departments ORDER BY name;

-- NATURAL LEFT JOIN
SELECT name, dept_name FROM employees NATURAL LEFT JOIN departments ORDER BY name;
