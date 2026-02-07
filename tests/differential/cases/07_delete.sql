CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO items VALUES (1, 'apple');
INSERT INTO items VALUES (2, 'banana');
INSERT INTO items VALUES (3, 'cherry');
DELETE FROM items WHERE id = 2;
SELECT * FROM items;
