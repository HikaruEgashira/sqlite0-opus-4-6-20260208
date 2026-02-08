-- Complex window function tests
CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, month INTEGER, amount INTEGER);
INSERT INTO sales VALUES (1, 'A', 1, 100);
INSERT INTO sales VALUES (2, 'A', 2, 150);
INSERT INTO sales VALUES (3, 'A', 3, 200);
INSERT INTO sales VALUES (4, 'B', 1, 80);
INSERT INTO sales VALUES (5, 'B', 2, 120);
INSERT INTO sales VALUES (6, 'B', 3, 90);

-- Running sum by product
SELECT product, month, amount, SUM(amount) OVER (PARTITION BY product ORDER BY month) AS running_total FROM sales ORDER BY product, month;

-- Rank within partition
SELECT product, month, amount, RANK() OVER (PARTITION BY product ORDER BY amount DESC) AS rank FROM sales ORDER BY product, rank;

-- ROW_NUMBER across all
SELECT product, month, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM sales ORDER BY rn;
