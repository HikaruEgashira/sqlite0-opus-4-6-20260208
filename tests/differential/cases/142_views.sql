-- Basic CREATE VIEW and DROP VIEW
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT);
INSERT INTO products VALUES (1, 'Apple', 100, 'fruit');
INSERT INTO products VALUES (2, 'Banana', 50, 'fruit');
INSERT INTO products VALUES (3, 'Carrot', 80, 'vegetable');
INSERT INTO products VALUES (4, 'Donut', 150, 'snack');
INSERT INTO products VALUES (5, 'Eggplant', 120, 'vegetable');

-- Create a view
CREATE VIEW fruit_view AS SELECT name, price FROM products WHERE category = 'fruit';
SELECT * FROM fruit_view;

-- Create view with expression
CREATE VIEW expensive_view AS SELECT name, price * 2 AS double_price FROM products WHERE price > 90;
SELECT * FROM expensive_view;

-- Drop view
DROP VIEW fruit_view;

-- Create view if not exists
CREATE VIEW IF NOT EXISTS expensive_view AS SELECT 1;
SELECT * FROM expensive_view;

-- Drop view if exists
DROP VIEW IF EXISTS expensive_view;
DROP VIEW IF EXISTS nonexistent_view;
