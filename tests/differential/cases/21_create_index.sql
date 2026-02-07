CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT);
INSERT INTO employees VALUES (1, 'alice', 'eng');
INSERT INTO employees VALUES (2, 'bob', 'sales');
INSERT INTO employees VALUES (3, 'charlie', 'eng');
CREATE INDEX idx_dept ON employees(dept);
SELECT * FROM employees WHERE dept = 'eng';
SELECT * FROM employees;
