CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, quantity INTEGER);
INSERT INTO items VALUES (1, 'widget', 42);
INSERT INTO items VALUES (2, 'gadget', 0);
INSERT INTO items VALUES (3, 'thing', 999);
SELECT * FROM items;
SELECT label FROM items;
SELECT id, quantity FROM items;
