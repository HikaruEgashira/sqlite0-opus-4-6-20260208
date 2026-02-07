CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER);
INSERT INTO sales VALUES (1, 'food', 100);
INSERT INTO sales VALUES (2, 'food', 200);
INSERT INTO sales VALUES (3, 'drink', 50);
INSERT INTO sales VALUES (4, 'drink', 75);
INSERT INTO sales VALUES (5, 'food', 150);
INSERT INTO sales VALUES (6, 'snack', 30);
SELECT category, COUNT(*) FROM sales GROUP BY category;
SELECT category, SUM(amount) FROM sales GROUP BY category;
