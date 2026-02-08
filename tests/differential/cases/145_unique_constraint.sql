-- UNIQUE constraint enforcement
CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);
INSERT INTO users VALUES (1, 'alice@test.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@test.com', 'Bob');
SELECT * FROM users ORDER BY id;

-- INSERT OR IGNORE with UNIQUE
INSERT OR IGNORE INTO users VALUES (3, 'alice@test.com', 'Duplicate');
SELECT * FROM users ORDER BY id;

-- INSERT OR IGNORE with new unique value
INSERT OR IGNORE INTO users VALUES (3, 'carol@test.com', 'Carol');
SELECT * FROM users ORDER BY id;

-- NULL values don't conflict with each other
CREATE TABLE t_nullable (id INTEGER PRIMARY KEY, code TEXT UNIQUE);
INSERT INTO t_nullable VALUES (1, NULL);
INSERT INTO t_nullable VALUES (2, NULL);
INSERT INTO t_nullable VALUES (3, 'A');
SELECT * FROM t_nullable ORDER BY id;
