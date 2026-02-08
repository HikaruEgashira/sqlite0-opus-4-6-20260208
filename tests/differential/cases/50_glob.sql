CREATE TABLE files (name TEXT);
INSERT INTO files VALUES ('readme.txt');
INSERT INTO files VALUES ('README.md');
INSERT INTO files VALUES ('data.csv');
INSERT INTO files VALUES ('script.sh');
INSERT INTO files VALUES ('test_file.txt');

SELECT name FROM files WHERE name GLOB '*.txt';

SELECT name FROM files WHERE name GLOB 'README*';

SELECT name FROM files WHERE name GLOB '????.*';

SELECT name FROM files WHERE name GLOB '*_*';
