CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO users VALUES (3, 'charlie', 30);
INSERT INTO users VALUES (1, 'alice', 25);
INSERT INTO users VALUES (2, 'bob', 28);
SELECT * FROM users ORDER BY id;
SELECT * FROM users ORDER BY age DESC;
SELECT * FROM users ORDER BY name;
