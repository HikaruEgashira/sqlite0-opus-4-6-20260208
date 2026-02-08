-- Aggregates on JOIN results
CREATE TABLE departments (id INTEGER, name TEXT);
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER);
INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');
INSERT INTO departments VALUES (3, 'HR');
INSERT INTO employees VALUES (1, 'Alice', 1, 80000);
INSERT INTO employees VALUES (2, 'Bob', 1, 90000);
INSERT INTO employees VALUES (3, 'Charlie', 2, 70000);

-- COUNT(*) on JOIN
SELECT COUNT(*) FROM employees e INNER JOIN departments d ON e.dept_id = d.id;

-- GROUP BY with JOIN and COUNT(*)
SELECT d.name, COUNT(*) FROM departments d INNER JOIN employees e ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name;

-- SUM with JOIN
SELECT d.name, SUM(salary) FROM departments d INNER JOIN employees e ON e.dept_id = d.id GROUP BY d.name ORDER BY d.name;

-- MAX with JOIN
SELECT MAX(salary) FROM employees e INNER JOIN departments d ON e.dept_id = d.id;
