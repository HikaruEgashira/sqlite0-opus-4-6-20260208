CREATE TABLE products (id INTEGER, name TEXT);
INSERT INTO products VALUES (1, 'apple');
INSERT INTO products VALUES (2, 'application');
INSERT INTO products VALUES (3, 'banana');
INSERT INTO products VALUES (4, 'Apricot');
SELECT id, name FROM products WHERE name LIKE 'app%' ORDER BY id;
SELECT id, name FROM products WHERE name LIKE '%a%' ORDER BY id;
SELECT id FROM products WHERE name LIKE 'a_ple' ORDER BY id;
SELECT id FROM products WHERE name LIKE 'A%' ORDER BY id;
