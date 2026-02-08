-- Window aggregate functions: SUM/AVG/COUNT/MIN/MAX OVER
CREATE TABLE sales (id INTEGER, dept TEXT, amount INTEGER);
INSERT INTO sales VALUES (1, 'A', 100);
INSERT INTO sales VALUES (2, 'A', 200);
INSERT INTO sales VALUES (3, 'B', 150);
INSERT INTO sales VALUES (4, 'A', 300);
INSERT INTO sales VALUES (5, 'B', 250);

-- SUM OVER (PARTITION BY)
SELECT id, dept, amount, SUM(amount) OVER (PARTITION BY dept) AS dept_total FROM sales ORDER BY id;

-- COUNT OVER (PARTITION BY)
SELECT id, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM sales ORDER BY id;

-- AVG OVER (PARTITION BY)
SELECT id, dept, amount, AVG(amount) OVER (PARTITION BY dept) AS dept_avg FROM sales ORDER BY id;

-- MIN/MAX OVER (PARTITION BY)
SELECT id, dept, amount, MIN(amount) OVER (PARTITION BY dept) AS dept_min, MAX(amount) OVER (PARTITION BY dept) AS dept_max FROM sales ORDER BY id;

-- SUM OVER () - entire table as one partition
SELECT id, amount, SUM(amount) OVER () AS total FROM sales ORDER BY id;
