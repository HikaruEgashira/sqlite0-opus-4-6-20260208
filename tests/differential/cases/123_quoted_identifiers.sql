-- Quoted identifiers (double quotes and backticks)
CREATE TABLE "my table" (id INTEGER, "full name" TEXT);
INSERT INTO "my table" VALUES (1, 'Alice Smith');
INSERT INTO "my table" VALUES (2, 'Bob Jones');
SELECT id, "full name" FROM "my table" ORDER BY id;
SELECT `full name` FROM `my table` WHERE id = 1;
DROP TABLE "my table";
