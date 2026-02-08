-- SUBSTR with negative index and negative length
SELECT SUBSTR('hello', -2);
SELECT SUBSTR('hello', -2, 4);
SELECT SUBSTR('hello', 3, -2);
SELECT SUBSTR('hello', -1, -2);
SELECT SUBSTR('hello', 0, 3);
SELECT SUBSTR('abcdef', -3, 2);
SELECT SUBSTR('abcdef', 2, -1);
