CREATE TABLE sales (id INTEGER, dept TEXT, region TEXT, amount INTEGER);
INSERT INTO sales VALUES (1, 'eng', 'east', 100);
INSERT INTO sales VALUES (2, 'eng', 'west', 200);
INSERT INTO sales VALUES (3, 'eng', 'east', 150);
INSERT INTO sales VALUES (4, 'sales', 'east', 300);
INSERT INTO sales VALUES (5, 'sales', 'west', 250);
INSERT INTO sales VALUES (6, 'hr', 'east', 50);

-- HAVING with AND
SELECT dept, SUM(amount), COUNT(*) FROM sales GROUP BY dept HAVING SUM(amount) > 100 AND COUNT(*) > 1;

-- HAVING with OR
SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 500 OR COUNT(*) = 1;

-- HAVING with aggregate not in SELECT
SELECT dept FROM sales GROUP BY dept HAVING COUNT(*) >= 2;

-- HAVING with expression (arithmetic)
SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) * 2 > 800;
