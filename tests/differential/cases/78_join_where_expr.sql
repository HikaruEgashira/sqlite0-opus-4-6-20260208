CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
INSERT INTO users VALUES (1, 'alice', 25);
INSERT INTO users VALUES (2, 'bob', 30);
INSERT INTO users VALUES (3, 'carol', 35);
INSERT INTO orders VALUES (10, 1, 100);
INSERT INTO orders VALUES (11, 2, 200);
INSERT INTO orders VALUES (12, 1, 150);
INSERT INTO orders VALUES (13, 3, 50);

-- JOIN with simple WHERE
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY o.id;

-- JOIN with AND in WHERE
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount >= 100 AND u.age < 35 ORDER BY o.id;

-- JOIN with OR in WHERE
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.name = 'alice' OR o.amount = 200 ORDER BY o.id;

-- LEFT JOIN with WHERE on right table
SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.amount IS NOT NULL ORDER BY o.id;

-- JOIN with function in WHERE
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE UPPER(u.name) = 'ALICE' ORDER BY o.id;
