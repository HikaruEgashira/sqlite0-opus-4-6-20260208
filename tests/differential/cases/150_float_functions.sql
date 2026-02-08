-- ABS and SIGN with float values
SELECT ABS(-3.5), ABS(3.5), ABS(-5), ABS(0);
SELECT SIGN(-3.5), SIGN(0), SIGN(2.5), SIGN(-7);
SELECT ABS(NULL), SIGN(NULL);

-- ROUND with various precisions
SELECT ROUND(3.14159, 0), ROUND(3.14159, 2), ROUND(3.14159, 4);
SELECT ROUND(-2.5), ROUND(2.5);

-- Math functions with edge cases
SELECT CEIL(3.0), FLOOR(3.0);
SELECT CEIL(-0.5), FLOOR(-0.5);
SELECT POWER(2, -1), POWER(0, 0);
