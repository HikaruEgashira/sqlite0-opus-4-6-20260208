-- Derived tables (subqueries in FROM)
CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT);
INSERT INTO items VALUES (1, 'Apple', 100, 'fruit');
INSERT INTO items VALUES (2, 'Banana', 50, 'fruit');
INSERT INTO items VALUES (3, 'Carrot', 80, 'vegetable');
INSERT INTO items VALUES (4, 'Donut', 150, 'snack');
INSERT INTO items VALUES (5, 'Eggplant', 120, 'vegetable');

-- Basic derived table
SELECT * FROM (SELECT name, price FROM items WHERE price > 90) sub;

-- Derived table with expression
SELECT * FROM (SELECT name, price * 2 AS doubled FROM items) sub WHERE doubled > 200;

-- Derived table with aggregation
SELECT * FROM (SELECT category, COUNT(*) AS cnt FROM items GROUP BY category) sub ORDER BY cnt DESC;

-- Nested: select specific columns from derived table
SELECT cnt FROM (SELECT category, COUNT(*) AS cnt FROM items GROUP BY category) sub WHERE cnt > 1;
