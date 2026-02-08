-- Recursive CTE (WITH RECURSIVE)
-- Generate a series 1 to 5
WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x FROM cnt;

-- Factorial
WITH RECURSIVE fact(n, f) AS (SELECT 1, 1 UNION ALL SELECT n+1, f*(n+1) FROM fact WHERE n < 6) SELECT n, f FROM fact;

-- Fibonacci
WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 50) SELECT a FROM fib;
