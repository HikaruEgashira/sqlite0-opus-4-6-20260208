-- Window functions with PARTITION BY
CREATE TABLE sales (id INTEGER, dept TEXT, amount INTEGER);
INSERT INTO sales VALUES (1, 'A', 100);
INSERT INTO sales VALUES (2, 'A', 200);
INSERT INTO sales VALUES (3, 'B', 150);
INSERT INTO sales VALUES (4, 'A', 300);
INSERT INTO sales VALUES (5, 'B', 250);
INSERT INTO sales VALUES (6, 'B', 100);

-- ROW_NUMBER with PARTITION BY
SELECT id, dept, amount, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount) AS rn FROM sales ORDER BY dept, rn;

-- RANK with PARTITION BY
CREATE TABLE scores (id INTEGER, team TEXT, score INTEGER);
INSERT INTO scores VALUES (1, 'X', 90);
INSERT INTO scores VALUES (2, 'X', 80);
INSERT INTO scores VALUES (3, 'X', 90);
INSERT INTO scores VALUES (4, 'Y', 70);
INSERT INTO scores VALUES (5, 'Y', 85);
INSERT INTO scores VALUES (6, 'Y', 85);

SELECT id, team, score, RANK() OVER (PARTITION BY team ORDER BY score DESC) AS rnk FROM scores ORDER BY team, rnk, id;

-- DENSE_RANK with PARTITION BY
SELECT id, team, score, DENSE_RANK() OVER (PARTITION BY team ORDER BY score DESC) AS drnk FROM scores ORDER BY team, drnk, id;
