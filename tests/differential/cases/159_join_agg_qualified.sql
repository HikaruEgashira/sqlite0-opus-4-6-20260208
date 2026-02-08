-- JOIN aggregates with qualified column references
CREATE TABLE users (id INTEGER, name TEXT);
CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150);

-- LEFT JOIN with COUNT(qualified_ref) - NULL handling
SELECT u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name;

-- LEFT JOIN with SUM(qualified_ref) - NULL for unmatched
SELECT u.name, SUM(o.amount) AS total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name;

-- LEFT JOIN with MAX/MIN
SELECT u.name, MAX(o.amount), MIN(o.amount) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name;

-- INNER JOIN with aggregate
SELECT u.name, COUNT(o.id) AS cnt FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY cnt DESC;

-- GROUP BY qualified column
SELECT u.name, SUM(o.amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY 1;
