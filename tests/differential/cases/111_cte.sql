-- Common Table Expressions (WITH)
CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT, salary INTEGER);
INSERT INTO employees VALUES (1, 'alice', 'eng', 100);
INSERT INTO employees VALUES (2, 'bob', 'eng', 120);
INSERT INTO employees VALUES (3, 'carol', 'sales', 90);
INSERT INTO employees VALUES (4, 'dave', 'sales', 110);
INSERT INTO employees VALUES (5, 'eve', 'eng', 130);

-- Simple CTE
WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'eng') SELECT * FROM eng ORDER BY name;

-- CTE with aggregation in main query
WITH dept_totals AS (SELECT dept, SUM(salary) AS total FROM employees GROUP BY dept) SELECT * FROM dept_totals ORDER BY dept;

-- Multiple CTEs
WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'eng'), sales AS (SELECT name, salary FROM employees WHERE dept = 'sales') SELECT * FROM eng ORDER BY name;

-- CTE used with WHERE
WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 100) SELECT name FROM high_earners ORDER BY name;
