-- Multiple JOIN patterns
CREATE TABLE departments (id INTEGER, name TEXT);
CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER);
CREATE TABLE projects (id INTEGER, name TEXT, lead_id INTEGER);

INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Sales');
INSERT INTO departments VALUES (3, 'HR');

INSERT INTO employees VALUES (1, 'Alice', 1);
INSERT INTO employees VALUES (2, 'Bob', 1);
INSERT INTO employees VALUES (3, 'Charlie', 2);
INSERT INTO employees VALUES (4, 'Diana', NULL);

INSERT INTO projects VALUES (1, 'Widget', 1);
INSERT INTO projects VALUES (2, 'Gadget', 3);

-- Three-way JOIN
SELECT e.name, d.name, p.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id INNER JOIN projects p ON p.lead_id = e.id ORDER BY e.name;

-- LEFT JOIN with NULL
SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name;

-- Two-table inner join
SELECT e.name, d.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.name;
