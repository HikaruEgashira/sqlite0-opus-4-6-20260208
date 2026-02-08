-- COALESCE and NULLIF edge cases
CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t1 VALUES (1, NULL, NULL, 'fallback');
INSERT INTO t1 VALUES (2, NULL, 'second', 'third');
INSERT INTO t1 VALUES (3, 'first', NULL, NULL);
INSERT INTO t1 VALUES (4, NULL, NULL, NULL);

-- COALESCE with multiple NULLs
SELECT id, COALESCE(a, b, c, 'default') FROM t1 ORDER BY id;

-- NULLIF returns NULL when args equal
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);
SELECT NULLIF('hello', 'hello');
SELECT NULLIF('hello', 'world');

-- COALESCE with expressions
SELECT COALESCE(NULL, 1 + 2);
SELECT COALESCE(NULL, NULL, LENGTH('hello'));

-- IIF with NULL
SELECT IIF(NULL, 'yes', 'no');
SELECT IIF(1, 'yes', 'no');
SELECT IIF(0, 'yes', 'no');
