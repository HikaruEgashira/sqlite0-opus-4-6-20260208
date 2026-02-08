-- LIKE() and GLOB() as scalar functions
SELECT LIKE('%bc', 'abc');
SELECT LIKE('a%', 'abc');
SELECT LIKE('%x%', 'abc');
SELECT LIKE('_b_', 'abc');
SELECT GLOB('*bc', 'abc');
SELECT GLOB('a*', 'abc');
SELECT GLOB('?b?', 'abc');
SELECT GLOB('*x*', 'abc');
SELECT LIKE('%', NULL);
SELECT GLOB('*', NULL);
