-- Simple CASE expression (CASE expr WHEN val THEN result)
CREATE TABLE items (id INTEGER, status INTEGER);
INSERT INTO items VALUES (1, 1);
INSERT INTO items VALUES (2, 2);
INSERT INTO items VALUES (3, 3);
INSERT INTO items VALUES (4, 1);

-- Simple CASE with integer operand
SELECT id, CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END AS label FROM items ORDER BY id;

-- Simple CASE with literal
SELECT CASE 'hello' WHEN 'hello' THEN 'matched' WHEN 'world' THEN 'nope' END;

-- Simple CASE without ELSE (returns NULL)
SELECT CASE 99 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;
