CREATE TABLE source (id INTEGER, name TEXT);
INSERT INTO source VALUES (1, 'alice');
INSERT INTO source VALUES (2, 'bob');
INSERT INTO source VALUES (3, 'charlie');

CREATE TABLE target (id INTEGER, name TEXT);
INSERT INTO target SELECT * FROM source;
SELECT * FROM target;
