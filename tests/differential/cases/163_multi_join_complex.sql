-- Complex multi-JOIN queries
CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL);
CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Marketing');
INSERT INTO departments VALUES (3, 'Sales');

INSERT INTO employees VALUES (1, 'Alice', 1, 95000.0);
INSERT INTO employees VALUES (2, 'Bob', 1, 85000.0);
INSERT INTO employees VALUES (3, 'Carol', 2, 75000.0);
INSERT INTO employees VALUES (4, 'Dave', 3, 65000.0);
INSERT INTO employees VALUES (5, 'Eve', 1, 90000.0);

INSERT INTO projects VALUES (1, 'Website', 1);
INSERT INTO projects VALUES (2, 'Campaign', 2);
INSERT INTO projects VALUES (3, 'Database', 1);

-- 3-table JOIN
SELECT e.name, d.name, p.name FROM employees e JOIN departments d ON e.dept_id = d.id JOIN projects p ON p.dept_id = d.id ORDER BY e.name, p.name;

-- JOIN with aggregate and ORDER BY
SELECT d.name, COUNT(e.id), ROUND(AVG(e.salary), 2) FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name;

-- JOIN DISTINCT with aggregate
SELECT DISTINCT d.name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.salary > 80000.0 ORDER BY d.name;
