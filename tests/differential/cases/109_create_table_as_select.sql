-- CREATE TABLE AS SELECT
CREATE TABLE src (id INTEGER, name TEXT, score INTEGER);
INSERT INTO src VALUES (1, 'alice', 90);
INSERT INTO src VALUES (2, 'bob', 85);
INSERT INTO src VALUES (3, 'carol', 95);

-- Basic CREATE TABLE AS SELECT
CREATE TABLE dst AS SELECT * FROM src;
SELECT * FROM dst ORDER BY id;

-- CREATE TABLE AS SELECT with column subset
CREATE TABLE names AS SELECT name FROM src ORDER BY name;
SELECT * FROM names;

-- CREATE TABLE AS SELECT with expression
CREATE TABLE scored AS SELECT name, score * 2 AS doubled FROM src ORDER BY name;
SELECT * FROM scored;

-- CREATE TABLE AS SELECT with WHERE
CREATE TABLE high AS SELECT name, score FROM src WHERE score >= 90 ORDER BY name;
SELECT * FROM high;
