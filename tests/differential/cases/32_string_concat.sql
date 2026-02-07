CREATE TABLE names (first TEXT, last TEXT);
INSERT INTO names VALUES ('Alice', 'Smith');
INSERT INTO names VALUES ('Bob', 'Jones');
SELECT first || ' ' || last FROM names;
SELECT first || last FROM names;
