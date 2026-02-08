-- Date/time functions with fixed values
SELECT date('2024-01-15');
SELECT time('2024-01-15 14:30:45');
SELECT datetime('2024-01-15 14:30:45');

-- Date extraction from datetime
SELECT date('2024-12-25 08:00:00');
SELECT time('2024-06-15 23:59:59');

-- Date-only input
SELECT date('2024-03-01');
SELECT datetime('2024-03-01');

-- strftime
SELECT strftime('%Y', '2024-01-15');
SELECT strftime('%m', '2024-01-15');
SELECT strftime('%d', '2024-01-15');
SELECT strftime('%H', '2024-01-15 14:30:45');
SELECT strftime('%M', '2024-01-15 14:30:45');
SELECT strftime('%S', '2024-01-15 14:30:45');
SELECT strftime('%Y-%m-%d', '2024-06-15');
