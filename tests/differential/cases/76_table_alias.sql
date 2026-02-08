CREATE TABLE users (id INTEGER, name TEXT);
CREATE TABLE orders (id INTEGER, user_id INTEGER, item TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
INSERT INTO users VALUES (3, 'carol');
INSERT INTO orders VALUES (10, 1, 'apple');
INSERT INTO orders VALUES (11, 2, 'banana');
INSERT INTO orders VALUES (12, 1, 'cherry');

-- Table alias in simple SELECT
SELECT u.id, u.name FROM users u ORDER BY u.id;

-- Table alias with AS keyword
SELECT u.id, u.name FROM users AS u ORDER BY u.id;

-- Table alias in JOIN with alias references
SELECT u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id;

-- Table alias in JOIN with AS keyword
SELECT u.name, o.item FROM users AS u JOIN orders AS o ON u.id = o.user_id ORDER BY o.id;

-- LEFT JOIN with alias
SELECT u.name, o.item FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id;
