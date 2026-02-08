-- CASE expression variations
CREATE TABLE t (id INTEGER, score INTEGER, grade TEXT);
INSERT INTO t VALUES (1, 95, NULL);
INSERT INTO t VALUES (2, 82, NULL);
INSERT INTO t VALUES (3, 67, NULL);
INSERT INTO t VALUES (4, 45, NULL);

-- Searched CASE
SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END AS grade FROM t ORDER BY id;

-- Simple CASE
SELECT id, CASE id WHEN 1 THEN 'first' WHEN 2 THEN 'second' ELSE 'other' END FROM t ORDER BY id;

-- CASE with NULL
SELECT id, CASE WHEN score IS NULL THEN 'no score' ELSE CAST(score AS TEXT) END FROM t ORDER BY id;

-- Nested CASE
SELECT id, CASE WHEN score >= 80 THEN CASE WHEN score >= 90 THEN 'excellent' ELSE 'good' END ELSE 'needs work' END FROM t ORDER BY id;
