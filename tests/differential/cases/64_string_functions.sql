CREATE TABLE t1 (id INTEGER, name TEXT);
INSERT INTO t1 VALUES (1, 'hello world');
INSERT INTO t1 VALUES (2, 'foobar');
INSERT INTO t1 VALUES (3, 'abcabc');

-- SUBSTR basic
SELECT SUBSTR('hello', 1, 3);
SELECT SUBSTR('hello', 2);

-- SUBSTR with negative start (from end)
SELECT SUBSTR('hello', -3);

-- INSTR
SELECT INSTR('hello world', 'world');
SELECT INSTR('hello world', 'xyz');
SELECT INSTR('abcabc', 'b');

-- REPLACE function
SELECT REPLACE('hello world', 'world', 'zig');
SELECT REPLACE('aabbcc', 'bb', 'XX');
SELECT REPLACE('hello', 'xyz', 'abc');

-- Combined with table
SELECT id, SUBSTR(name, 1, 5) FROM t1 ORDER BY id;
SELECT id, INSTR(name, 'o') FROM t1 ORDER BY id;
SELECT id, REPLACE(name, 'o', '0') FROM t1 ORDER BY id;
