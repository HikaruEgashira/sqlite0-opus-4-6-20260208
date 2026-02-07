CREATE TABLE empty_t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);
SELECT * FROM empty_t;
SELECT COUNT(*) FROM empty_t;
SELECT COUNT(name) FROM empty_t;
SELECT MIN(value) FROM empty_t;
SELECT MAX(value) FROM empty_t;
SELECT SUM(value) FROM empty_t;
SELECT AVG(value) FROM empty_t;
