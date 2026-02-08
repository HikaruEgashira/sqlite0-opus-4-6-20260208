CREATE TABLE sales (id INTEGER, dept TEXT, region TEXT, amount INTEGER);
INSERT INTO sales VALUES (1, 'eng', 'east', 100);
INSERT INTO sales VALUES (2, 'eng', 'west', 200);
INSERT INTO sales VALUES (3, 'eng', 'east', 150);
INSERT INTO sales VALUES (4, 'sales', 'east', 300);
INSERT INTO sales VALUES (5, 'sales', 'west', 250);
INSERT INTO sales VALUES (6, 'sales', 'east', 350);

-- Single GROUP BY (baseline)
SELECT dept, SUM(amount) FROM sales GROUP BY dept;

-- Multi-column GROUP BY
SELECT dept, region, SUM(amount) FROM sales GROUP BY dept, region;

-- Multi-column GROUP BY with COUNT
SELECT dept, region, COUNT(*) FROM sales GROUP BY dept, region;

-- Multi-column GROUP BY with ORDER BY
SELECT dept, region, SUM(amount) FROM sales GROUP BY dept, region ORDER BY dept, region;
