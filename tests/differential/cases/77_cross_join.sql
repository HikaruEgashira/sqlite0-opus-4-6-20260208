CREATE TABLE colors (id INTEGER, color TEXT);
CREATE TABLE sizes (id INTEGER, size TEXT);
INSERT INTO colors VALUES (1, 'red');
INSERT INTO colors VALUES (2, 'blue');
INSERT INTO sizes VALUES (1, 'S');
INSERT INTO sizes VALUES (2, 'M');
INSERT INTO sizes VALUES (3, 'L');

-- CROSS JOIN (explicit)
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s ORDER BY c.id, s.id;

-- Comma syntax (implicit CROSS JOIN)
SELECT colors.color, sizes.size FROM colors, sizes ORDER BY colors.id, sizes.id;

-- CROSS JOIN with WHERE filter
SELECT c.color, s.size FROM colors c CROSS JOIN sizes s WHERE s.size = 'M' ORDER BY c.id;
