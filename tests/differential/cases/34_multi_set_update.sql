CREATE TABLE users (id INTEGER, age INTEGER, score INTEGER);
INSERT INTO users VALUES (1, 30, 100);
INSERT INTO users VALUES (2, 25, 95);
UPDATE users SET age = 31, score = 101 WHERE id = 1;
SELECT * FROM users ORDER BY id;
UPDATE users SET score = 96, age = 26 WHERE id = 2;
SELECT * FROM users ORDER BY id;
