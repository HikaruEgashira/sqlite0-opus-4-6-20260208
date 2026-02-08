-- String operation edge cases
SELECT LENGTH('');
SELECT LENGTH(NULL);
SELECT UPPER(NULL);
SELECT LOWER(NULL);
SELECT TRIM('   ');
SELECT TRIM('  hello  ');
SELECT LTRIM('  hello');
SELECT RTRIM('hello  ');
SELECT REPLACE('hello', 'l', 'r');
SELECT REPLACE('hello', 'xyz', 'abc');
SELECT SUBSTR('', 1, 5);
SELECT INSTR('hello hello', 'hello');
SELECT INSTR('hello', 'xyz');
