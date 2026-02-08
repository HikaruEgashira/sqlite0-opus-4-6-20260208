-- Correlated subqueries
CREATE TABLE departments (id INTEGER, name TEXT);
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR');

CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary INTEGER);
INSERT INTO employees VALUES (1, 'Alice', 1, 100), (2, 'Bob', 1, 120);
INSERT INTO employees VALUES (3, 'Carol', 2, 80), (4, 'Dave', 2, 90);
INSERT INTO employees VALUES (5, 'Eve', 3, 110);

-- Scalar correlated subquery: count employees per department
SELECT d.name, (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count FROM departments d ORDER BY d.id;

-- Correlated subquery with SUM
SELECT d.name, (SELECT SUM(e.salary) FROM employees e WHERE e.dept_id = d.id) AS total_sal FROM departments d ORDER BY d.id;

-- Correlated subquery with MAX
SELECT d.name, (SELECT MAX(e.salary) FROM employees e WHERE e.dept_id = d.id) AS max_sal FROM departments d ORDER BY d.id;

-- EXISTS with correlated subquery
SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 100) ORDER BY d.name;

-- NOT EXISTS with correlated subquery
SELECT d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 200) ORDER BY d.name;

-- IN with correlated subquery (departments that have employees)
SELECT name FROM departments WHERE id IN (SELECT dept_id FROM employees) ORDER BY name;

-- Correlated subquery in WHERE
SELECT e.name, e.salary FROM employees e WHERE e.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id) ORDER BY e.name;

-- Scalar subquery returning name
SELECT e.name, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name FROM employees e ORDER BY e.id;
