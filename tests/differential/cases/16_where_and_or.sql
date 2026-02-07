CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT);
INSERT INTO products VALUES (1, 'apple', 100, 'fruit');
INSERT INTO products VALUES (2, 'banana', 150, 'fruit');
INSERT INTO products VALUES (3, 'carrot', 80, 'vegetable');
INSERT INTO products VALUES (4, 'date', 200, 'fruit');
INSERT INTO products VALUES (5, 'eggplant', 120, 'vegetable');
SELECT * FROM products WHERE category = 'fruit' AND price > 100;
SELECT * FROM products WHERE category = 'fruit' OR category = 'vegetable';
SELECT * FROM products WHERE price >= 100 AND price <= 150;
