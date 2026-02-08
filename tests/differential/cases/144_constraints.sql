-- Table constraints: FOREIGN KEY, UNIQUE, REFERENCES, composite PRIMARY KEY
CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO parent VALUES (1, 'Alice');
INSERT INTO parent VALUES (2, 'Bob');

-- Column-level REFERENCES
CREATE TABLE child1 (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO child1 VALUES (1, 1, 'x');
INSERT INTO child1 VALUES (2, 2, 'y');
SELECT * FROM child1;

-- Column-level UNIQUE
CREATE TABLE t_unique (id INTEGER PRIMARY KEY, code TEXT UNIQUE);
INSERT INTO t_unique VALUES (1, 'A');
INSERT INTO t_unique VALUES (2, 'B');
SELECT * FROM t_unique;

-- Table-level FOREIGN KEY
CREATE TABLE child2 (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id));
INSERT INTO child2 VALUES (1, 1);
SELECT * FROM child2;

-- Table-level UNIQUE
CREATE TABLE t_multi_unique (a INTEGER, b TEXT, UNIQUE(a, b));
INSERT INTO t_multi_unique VALUES (1, 'x');
SELECT * FROM t_multi_unique;

-- Composite PRIMARY KEY
CREATE TABLE t_cpk (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_cpk VALUES (1, 'x', 10);
INSERT INTO t_cpk VALUES (2, 'y', 20);
SELECT * FROM t_cpk ORDER BY a;

-- Column-level NOT NULL + UNIQUE combo
CREATE TABLE t_combo (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
INSERT INTO t_combo VALUES (1, 'Alice');
SELECT * FROM t_combo;
