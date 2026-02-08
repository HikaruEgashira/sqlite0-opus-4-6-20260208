-- Deeply nested function calls
SELECT ABS(ABS(ABS(-42)));
SELECT UPPER(LOWER(UPPER('Hello')));
SELECT LENGTH(SUBSTR('hello world', 1, 5));
SELECT COALESCE(NULL, COALESCE(NULL, 'found'));
SELECT IIF(1 > 0, UPPER('yes'), LOWER('NO'));
SELECT REPLACE(UPPER('hello world'), 'WORLD', LOWER('THERE'));
SELECT TRIM(SUBSTR('  hello  ', 3, 5));
SELECT CAST(LENGTH('abc') + LENGTH('de') AS TEXT);
