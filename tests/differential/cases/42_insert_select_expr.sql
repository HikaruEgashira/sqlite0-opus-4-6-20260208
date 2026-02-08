CREATE TABLE data (id INTEGER, val INTEGER);
INSERT INTO data VALUES (1, 10);
INSERT INTO data VALUES (2, 20);
INSERT INTO data VALUES (3, 30);

CREATE TABLE doubled (id INTEGER, val INTEGER);
INSERT INTO doubled SELECT id, val * 2 FROM data;
SELECT * FROM doubled;
CREATE TABLE summary (total_count INTEGER, total_sum INTEGER);
INSERT INTO summary SELECT COUNT(*), SUM(val) FROM data;
SELECT * FROM summary;
