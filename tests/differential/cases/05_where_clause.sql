CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
INSERT INTO users VALUES (3, 'charlie');
SELECT * FROM users WHERE id = 2;
SELECT name FROM users WHERE id = 1;
SELECT * FROM users WHERE name = 'charlie';
