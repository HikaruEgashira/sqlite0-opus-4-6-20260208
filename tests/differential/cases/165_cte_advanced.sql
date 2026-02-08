-- Advanced CTE tests
CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER, salary INTEGER);
INSERT INTO employees VALUES (1, 'CEO', NULL, 200000);
INSERT INTO employees VALUES (2, 'VP1', 1, 150000);
INSERT INTO employees VALUES (3, 'VP2', 1, 140000);
INSERT INTO employees VALUES (4, 'Mgr1', 2, 100000);
INSERT INTO employees VALUES (5, 'Mgr2', 3, 95000);
INSERT INTO employees VALUES (6, 'Dev1', 4, 80000);

-- Multiple CTEs
WITH high_earners AS (SELECT * FROM employees WHERE salary >= 100000), managers AS (SELECT DISTINCT manager_id FROM employees WHERE manager_id IS NOT NULL) SELECT h.name FROM high_earners h WHERE h.id IN (SELECT manager_id FROM managers) ORDER BY h.name;

-- CTE with aggregation
WITH dept_totals AS (SELECT manager_id, COUNT(*) AS cnt, SUM(salary) AS total FROM employees WHERE manager_id IS NOT NULL GROUP BY manager_id) SELECT e.name, d.cnt, d.total FROM employees e JOIN dept_totals d ON e.id = d.manager_id ORDER BY e.name;
