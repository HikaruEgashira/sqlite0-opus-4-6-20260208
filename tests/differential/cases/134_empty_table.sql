-- Empty table edge cases
CREATE TABLE t (id INTEGER, val TEXT);

-- Select from empty table
SELECT * FROM t;
SELECT COUNT(*) FROM t;
SELECT SUM(id) FROM t;
SELECT MAX(val) FROM t;

-- Insert and verify
INSERT INTO t VALUES (1, 'hello');
SELECT * FROM t;
SELECT COUNT(*) FROM t;
