-- Qualified references in aggregate functions
CREATE TABLE orders (id INTEGER, customer TEXT, amount INTEGER);
INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Alice', 200), (3, 'Bob', 150);

-- SUM with qualified ref
SELECT o.customer, SUM(o.amount) FROM orders o GROUP BY o.customer ORDER BY o.customer;

-- COUNT with qualified ref
SELECT o.customer, COUNT(o.id) FROM orders o GROUP BY o.customer ORDER BY 1;

-- AVG with qualified ref
SELECT o.customer, AVG(o.amount) FROM orders o GROUP BY o.customer ORDER BY 1;

-- MAX/MIN with qualified ref
SELECT o.customer, MAX(o.amount), MIN(o.amount) FROM orders o GROUP BY o.customer ORDER BY 1;

-- Table alias without qualified ref (should still work)
SELECT customer, SUM(amount) FROM orders o GROUP BY customer ORDER BY 1;
