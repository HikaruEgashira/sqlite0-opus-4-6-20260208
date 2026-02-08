-- Window functions: ROW_NUMBER, RANK, DENSE_RANK
CREATE TABLE scores (name TEXT, subject TEXT, score INTEGER);
INSERT INTO scores VALUES ('alice', 'math', 90);
INSERT INTO scores VALUES ('bob', 'math', 85);
INSERT INTO scores VALUES ('carol', 'math', 90);
INSERT INTO scores VALUES ('alice', 'english', 80);
INSERT INTO scores VALUES ('bob', 'english', 95);
INSERT INTO scores VALUES ('carol', 'english', 85);

-- ROW_NUMBER with ORDER BY
SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM scores WHERE subject = 'math' ORDER BY rn;

-- RANK with ORDER BY (ties get same rank, next rank skips)
SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores WHERE subject = 'math' ORDER BY rnk, name;

-- DENSE_RANK with ORDER BY (ties get same rank, no skipping)
SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores WHERE subject = 'math' ORDER BY drnk, name;
