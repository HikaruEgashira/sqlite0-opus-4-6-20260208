-- Multiline CTE test (tests REPL multiline support)
CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO items VALUES (1, 'Widget', 10);
INSERT INTO items VALUES (2, 'Gadget', 25);
INSERT INTO items VALUES (3, 'Doohickey', 5);
INSERT INTO items VALUES (4, 'Thingamajig', 15);

WITH expensive AS (
  SELECT * FROM items WHERE price > 10
)
SELECT name, price FROM expensive ORDER BY price DESC;

SELECT name,
  price
  FROM items
  WHERE price > 10
  ORDER BY name;
