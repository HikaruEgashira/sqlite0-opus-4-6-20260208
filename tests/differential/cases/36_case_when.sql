CREATE TABLE products (id INTEGER, code TEXT);
INSERT INTO products VALUES (1, 'ABC123');
INSERT INTO products VALUES (2, 'DEF456');
INSERT INTO products VALUES (3, 'ABC789');
INSERT INTO products VALUES (4, 'XYZ123');
SELECT id, code FROM products WHERE code LIKE 'ABC%' ORDER BY id;
SELECT id FROM products WHERE code LIKE '%123' ORDER BY id;
