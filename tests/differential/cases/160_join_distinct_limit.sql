-- JOIN DISTINCT and LIMIT/OFFSET tests
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER);
CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);

INSERT INTO categories VALUES (1, 'Electronics');
INSERT INTO categories VALUES (2, 'Books');
INSERT INTO categories VALUES (3, 'Clothing');

INSERT INTO products VALUES (1, 'Laptop', 1);
INSERT INTO products VALUES (2, 'Phone', 1);
INSERT INTO products VALUES (3, 'Tablet', 1);
INSERT INTO products VALUES (4, 'Novel', 2);
INSERT INTO products VALUES (5, 'Textbook', 2);
INSERT INTO products VALUES (6, 'T-shirt', 3);

-- DISTINCT on JOIN
SELECT DISTINCT c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY c.name;

-- JOIN with LIMIT
SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY p.name LIMIT 3;

-- JOIN with LIMIT and OFFSET
SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY p.name LIMIT 2 OFFSET 2;

-- SELECT * JOIN with LIMIT
SELECT * FROM products JOIN categories ON products.category_id = categories.id ORDER BY products.id LIMIT 2;

-- DISTINCT with multiple columns
SELECT DISTINCT category_id FROM products ORDER BY category_id;
