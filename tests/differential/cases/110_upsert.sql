-- UPSERT: INSERT ... ON CONFLICT
CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT);
INSERT INTO kv VALUES ('a', 'alpha');
INSERT INTO kv VALUES ('b', 'beta');

-- ON CONFLICT DO NOTHING (skip duplicate)
INSERT INTO kv VALUES ('a', 'new_alpha') ON CONFLICT(k) DO NOTHING;
SELECT * FROM kv ORDER BY k;

-- ON CONFLICT DO UPDATE
INSERT INTO kv VALUES ('b', 'bravo') ON CONFLICT(k) DO UPDATE SET v = excluded.v;
SELECT * FROM kv ORDER BY k;

-- Insert new row (no conflict)
INSERT INTO kv VALUES ('c', 'charlie') ON CONFLICT(k) DO UPDATE SET v = excluded.v;
SELECT * FROM kv ORDER BY k;
