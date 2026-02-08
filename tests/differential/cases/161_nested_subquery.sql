-- Nested subquery tests
CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL);

INSERT INTO regions VALUES (1, 'North');
INSERT INTO regions VALUES (2, 'South');
INSERT INTO regions VALUES (3, 'East');

INSERT INTO customers VALUES (1, 'Alice', 1);
INSERT INTO customers VALUES (2, 'Bob', 1);
INSERT INTO customers VALUES (3, 'Carol', 2);
INSERT INTO customers VALUES (4, 'Dave', 3);

INSERT INTO orders VALUES (1, 1, 100.0);
INSERT INTO orders VALUES (2, 1, 200.0);
INSERT INTO orders VALUES (3, 2, 150.0);
INSERT INTO orders VALUES (4, 3, 300.0);

-- Subquery in WHERE with IN
SELECT name FROM customers WHERE region_id IN (SELECT id FROM regions WHERE name = 'North') ORDER BY name;

-- Nested IN subquery
SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100.0) ORDER BY name;

-- EXISTS subquery
SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount >= 200.0) ORDER BY c.name;

-- NOT EXISTS subquery
SELECT c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY c.name;

-- Scalar subquery in SELECT
SELECT name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = customers.id) AS order_count FROM customers ORDER BY name;
