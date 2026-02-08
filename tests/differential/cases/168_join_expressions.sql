-- JOIN with expression evaluation in SELECT
CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER, dept_id INTEGER);
CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER);
INSERT INTO dept VALUES (1, 'Engineering', 500000);
INSERT INTO dept VALUES (2, 'Marketing', 300000);
INSERT INTO emp VALUES (1, 'Alice', 100, 1);
INSERT INTO emp VALUES (2, 'Bob', 80, 2);
INSERT INTO emp VALUES (3, 'Carol', 120, 1);

-- Expression in SELECT with JOIN
SELECT emp.name, emp.salary * 12 AS annual, dept.name AS department FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.name;

-- Function in JOIN SELECT
SELECT UPPER(emp.name), dept.name FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.name;

-- Arithmetic expression with both table columns
SELECT emp.name, emp.salary * 100 / dept.budget AS pct FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.name;

-- CASE expression in JOIN SELECT
SELECT emp.name, CASE WHEN emp.salary > 100 THEN 'high' ELSE 'normal' END AS level FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.name;

-- Concatenation in JOIN SELECT
SELECT emp.name || ' (' || dept.name || ')' AS full_name FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.name;
