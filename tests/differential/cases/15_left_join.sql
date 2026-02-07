CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
INSERT INTO users VALUES (3, 'carol');
INSERT INTO orders VALUES (1, 1, 'apple');
INSERT INTO orders VALUES (2, 1, 'banana');
INSERT INTO orders VALUES (3, 2, 'cherry');
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;
