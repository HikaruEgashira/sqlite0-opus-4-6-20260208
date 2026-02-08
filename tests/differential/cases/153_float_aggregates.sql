-- Aggregate functions with float values
CREATE TABLE t1 (id INTEGER, amount REAL);
INSERT INTO t1 VALUES (1, 10.5), (2, 20.75), (3, 30.25);

-- SUM on float column
SELECT SUM(amount) FROM t1;

-- AVG on float column
SELECT AVG(amount) FROM t1;

-- TOTAL on float column
SELECT TOTAL(amount) FROM t1;

-- MIN/MAX on float column
SELECT MIN(amount), MAX(amount) FROM t1;

-- GROUP BY with float aggregate
CREATE TABLE orders (customer TEXT, amount REAL);
INSERT INTO orders VALUES ('Alice', 100.50), ('Bob', 200.75), ('Alice', 50.25), ('Bob', 300.00);
SELECT customer, SUM(amount) FROM orders GROUP BY customer ORDER BY 1;
SELECT customer, AVG(amount) FROM orders GROUP BY customer ORDER BY 1;

-- Mixed integer and float in TEXT column
CREATE TABLE mixed (val TEXT);
INSERT INTO mixed VALUES ('10'), ('20.5'), ('30');
SELECT SUM(val), AVG(val) FROM mixed;

-- Float with NULL
INSERT INTO t1 VALUES (4, NULL);
SELECT SUM(amount), AVG(amount), COUNT(amount) FROM t1;
