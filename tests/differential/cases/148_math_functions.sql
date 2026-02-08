-- Math functions (SQLite 3.35+)
SELECT ceil(3.2), ceil(-3.2), ceil(5);
SELECT floor(3.8), floor(-3.8), floor(5);
SELECT sqrt(16), sqrt(2), sqrt(0);
SELECT power(2, 10), power(3, 0), power(4, 0.5);
SELECT log(100), log(1000), log(1);
SELECT log(2, 8), log(10, 1000);
SELECT log2(8), log2(1);
SELECT ln(1), ln(2.718281828);
SELECT exp(0), exp(1);
SELECT pi();
SELECT sin(0), cos(0), tan(0);
SELECT acos(1), asin(0), atan(0);
SELECT atan2(1, 1), atan2(0, 1);
SELECT mod(10, 3), mod(7.5, 2);
-- NULL handling
SELECT ceil(NULL), floor(NULL), sqrt(NULL), power(NULL, 2), log(NULL);
-- Domain errors return NULL
SELECT sqrt(-1), log(-1), ln(0), acos(2), asin(-2);
