-- Complex expression combinations
-- Nested function calls
SELECT ABS(LENGTH('hello') - 10);
SELECT UPPER(SUBSTR('hello world', 1, 5));
SELECT COALESCE(NULL, LENGTH('abc'));
SELECT MAX(ABS(-5), ABS(-3), ABS(-7));

-- Arithmetic in SELECT
SELECT 1 + 2 * 3;
SELECT (1 + 2) * 3;
SELECT 10 % 3;
SELECT 10 / 3;

-- String concatenation with expressions
SELECT 'Count: ' || CAST(1 + 2 AS TEXT);
SELECT UPPER('hello') || ' ' || LOWER('WORLD');
