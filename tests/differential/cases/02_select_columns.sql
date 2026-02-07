CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO products VALUES (1, 'apple', 100);
INSERT INTO products VALUES (2, 'banana', 200);
INSERT INTO products VALUES (3, 'cherry', 150);
SELECT name, price FROM products;
SELECT name FROM products;
SELECT * FROM products;
