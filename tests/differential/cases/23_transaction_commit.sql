CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);
INSERT INTO accounts VALUES (1, 100);
INSERT INTO accounts VALUES (2, 200);
BEGIN;
UPDATE accounts SET balance = 50 WHERE id = 1;
UPDATE accounts SET balance = 250 WHERE id = 2;
COMMIT;
SELECT * FROM accounts;
