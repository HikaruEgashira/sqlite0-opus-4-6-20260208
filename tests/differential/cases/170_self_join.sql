-- Self-join tests
CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER);
INSERT INTO employees VALUES (1, 'CEO', NULL);
INSERT INTO employees VALUES (2, 'VP', 1);
INSERT INTO employees VALUES (3, 'Manager', 2);
INSERT INTO employees VALUES (4, 'Developer', 3);
INSERT INTO employees VALUES (5, 'Tester', 3);

-- Self-join: employee with manager name
SELECT e.name AS employee, m.name AS manager FROM employees e JOIN employees m ON e.manager_id = m.id ORDER BY e.name;

-- Self-join with LEFT JOIN (include employees without managers)
SELECT e.name AS employee, COALESCE(m.name, 'None') AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.name;

-- Count direct reports per manager
SELECT m.name, COUNT(e.id) AS reports FROM employees m LEFT JOIN employees e ON e.manager_id = m.id GROUP BY m.name ORDER BY reports DESC, m.name;
