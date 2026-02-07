CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO products VALUES (1, 'widget', 100);
INSERT INTO products VALUES (2, 'gadget', 200);
INSERT INTO products VALUES (3, 'doohickey', 300);
UPDATE products SET price = 150 WHERE id = 1;
SELECT * FROM products;
UPDATE products SET name = 'gizmo' WHERE price = 200;
SELECT * FROM products;
