-- Complex SQL pattern tests
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT);
INSERT INTO products VALUES (1, 'Laptop', 999, 'Electronics');
INSERT INTO products VALUES (2, 'Phone', 699, 'Electronics');
INSERT INTO products VALUES (3, 'Tablet', 499, 'Electronics');
INSERT INTO products VALUES (4, 'Book', 29, 'Books');
INSERT INTO products VALUES (5, 'Pen', 2, 'Office');
INSERT INTO products VALUES (6, 'Desk', 299, 'Office');

-- Subquery in SELECT
SELECT name, price, (SELECT AVG(price) FROM products) AS avg_price FROM products WHERE price > 500 ORDER BY name;

-- CASE with aggregate
SELECT category, COUNT(*) AS cnt, CASE WHEN COUNT(*) > 2 THEN 'many' WHEN COUNT(*) = 2 THEN 'couple' ELSE 'few' END AS size FROM products GROUP BY category ORDER BY category;

-- Multiple aggregates
SELECT category, MIN(price) AS cheapest, MAX(price) AS most_expensive, SUM(price) AS total FROM products GROUP BY category ORDER BY category;

-- Nested function calls
SELECT name, LENGTH(UPPER(name)) AS len FROM products ORDER BY len DESC LIMIT 3;

-- BETWEEN with ORDER BY
SELECT name, price FROM products WHERE price BETWEEN 100 AND 700 ORDER BY price;

-- IN with multiple values
SELECT name FROM products WHERE category IN ('Electronics', 'Office') AND price < 500 ORDER BY name;
