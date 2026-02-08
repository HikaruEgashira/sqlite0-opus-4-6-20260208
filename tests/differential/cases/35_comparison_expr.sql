CREATE TABLE products (id INTEGER, price INTEGER);
INSERT INTO products VALUES (1, 50);
INSERT INTO products VALUES (2, 150);
INSERT INTO products VALUES (3, 100);
SELECT id FROM products WHERE price > 100 ORDER BY id;
SELECT id FROM products WHERE price = 100 ORDER BY id;
SELECT id FROM products WHERE price < 120 ORDER BY id;
SELECT id FROM products WHERE price >= 100 ORDER BY id;
