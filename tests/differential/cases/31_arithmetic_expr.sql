CREATE TABLE calc (a INTEGER, b INTEGER);
INSERT INTO calc VALUES (10, 3);
INSERT INTO calc VALUES (20, 5);
INSERT INTO calc VALUES (7, 2);
SELECT a + b FROM calc;
SELECT a - b FROM calc;
SELECT a * b FROM calc;
SELECT a + b * 2 FROM calc;
SELECT (a + b) * 2 FROM calc;
