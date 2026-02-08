-- Nested function call tests
SELECT UPPER(SUBSTR('hello world', 1, 5));
SELECT LENGTH(TRIM('  hello  '));
SELECT ABS(LENGTH('test') - 10);
SELECT COALESCE(NULL, UPPER('fallback'));
SELECT IFNULL(NULL, LENGTH('test'));
SELECT REPLACE(UPPER('hello'), 'HELLO', 'bye');
