-- LIMIT offset, count syntax tests
CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO items VALUES (1, 'a');
INSERT INTO items VALUES (2, 'b');
INSERT INTO items VALUES (3, 'c');
INSERT INTO items VALUES (4, 'd');
INSERT INTO items VALUES (5, 'e');
INSERT INTO items VALUES (6, 'f');

-- LIMIT offset, count (skip 2, take 3)
SELECT * FROM items LIMIT 2, 3;

-- LIMIT count OFFSET offset (equivalent)
SELECT * FROM items LIMIT 3 OFFSET 2;

-- LIMIT 0, count (no skip)
SELECT * FROM items LIMIT 0, 3;

-- LIMIT offset, large count (take all remaining)
SELECT * FROM items LIMIT 4, 100;

-- LIMIT with ORDER BY
SELECT * FROM items ORDER BY name DESC LIMIT 1, 3;
