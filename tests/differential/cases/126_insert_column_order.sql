-- INSERT with column reordering
CREATE TABLE t (id INTEGER, name TEXT, score INTEGER DEFAULT 0);
INSERT INTO t (name, id) VALUES ('Alice', 1);
INSERT INTO t (score, name, id) VALUES (100, 'Bob', 2);
INSERT INTO t (id, name) VALUES (3, 'Charlie');
SELECT id, name, score FROM t ORDER BY id;
