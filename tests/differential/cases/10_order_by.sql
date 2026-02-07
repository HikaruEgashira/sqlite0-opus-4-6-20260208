CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO products VALUES (1, 'banana', 150);
INSERT INTO products VALUES (2, 'apple', 100);
INSERT INTO products VALUES (3, 'cherry', 300);
INSERT INTO products VALUES (4, 'date', 200);
SELECT * FROM products ORDER BY price ASC;
SELECT * FROM products ORDER BY price DESC;
SELECT * FROM products ORDER BY name ASC;
SELECT name, price FROM products ORDER BY price ASC;
