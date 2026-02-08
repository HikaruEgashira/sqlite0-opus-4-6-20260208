CREATE TABLE t (id INTEGER, name TEXT);
INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

SELECT * FROM t;

INSERT INTO t VALUES (4, 'dave'), (5, 'eve');
SELECT * FROM t;
