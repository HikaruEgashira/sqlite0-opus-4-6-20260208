-- RIGHT JOIN tests
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');

CREATE TABLE orders (id INTEGER, user_id INTEGER, item TEXT);
INSERT INTO orders VALUES (1, 1, 'book');
INSERT INTO orders VALUES (2, 1, 'pen');
INSERT INTO orders VALUES (3, 3, 'cup');

-- RIGHT JOIN: all orders, NULLs for users without match
SELECT users.name, orders.item FROM users RIGHT JOIN orders ON users.id = orders.user_id;
