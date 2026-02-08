CREATE TABLE customers (id INTEGER, name TEXT);
INSERT INTO customers VALUES (1, 'Alice');
INSERT INTO customers VALUES (2, 'Bob');
INSERT INTO customers VALUES (3, 'Carol');

CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, qty INTEGER);
INSERT INTO orders VALUES (10, 1, 100, 2);
INSERT INTO orders VALUES (11, 1, 101, 1);
INSERT INTO orders VALUES (12, 2, 100, 3);
INSERT INTO orders VALUES (13, 3, 102, 1);

CREATE TABLE products (id INTEGER, pname TEXT, price INTEGER);
INSERT INTO products VALUES (100, 'Widget', 10);
INSERT INTO products VALUES (101, 'Gadget', 25);
INSERT INTO products VALUES (102, 'Doohickey', 50);

-- Three-table JOIN
SELECT c.name, p.pname, o.qty FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id ORDER BY o.id;

-- Three-table JOIN with WHERE
SELECT c.name, p.pname, o.qty FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id WHERE o.qty > 1 ORDER BY o.id;

-- Three-table JOIN selecting all
SELECT o.id, c.name, p.pname, p.price, o.qty FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id ORDER BY o.id;
