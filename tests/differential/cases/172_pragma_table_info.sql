-- PRAGMA table_info tests
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER DEFAULT 0);

-- Basic table_info
PRAGMA table_info(users);

-- Table with single column
CREATE TABLE simple (val TEXT);
PRAGMA table_info(simple);

-- Table with multiple constraints
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price INTEGER DEFAULT 100);
PRAGMA table_info(products);
