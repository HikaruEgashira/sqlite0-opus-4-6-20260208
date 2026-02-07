# ADR-0002: 段階的SQL互換性の拡大

## ステータス
採用

## コンテキスト
SQLite3は膨大なSQL機能を持つ。完全互換を一度に目指すとスコープが発散する。

## 決定
段階的にSQL互換性を拡大する方針を採る。

Phase 1 (完了):
- `CREATE TABLE` (INTEGER, TEXT のみ)
- `INSERT INTO ... VALUES`
- `SELECT * FROM` / `SELECT col1, col2 FROM`

Phase 2 (完了):
- `WHERE` 句
- `DELETE` / `UPDATE`
- `DROP TABLE`

Phase 3 (完了):
- `JOIN` (INNER, LEFT)
- `ORDER BY` / `GROUP BY`
- 集約関数 (COUNT, SUM, AVG, MIN, MAX)
- `LIMIT` / `OFFSET`

Phase 4 (完了):
- `DISTINCT`
- `HAVING` 句
- `ALTER TABLE` (ADD COLUMN, RENAME TO)
- `CREATE INDEX` (構文サポート)
- サブクエリ (IN / スカラー)

Phase 5 (完了):
- `BEGIN` / `COMMIT` / `ROLLBACK` (インメモリスナップショット方式)
- コアのリファクタリング (executeSelect / projectColumns 分割)

Phase 6 (次):
- B-Treeベースのストレージ
- SQLite3互換ファイルフォーマット

## 理由
- 各フェーズでDifferential Testingにより動作を検証できる
- 小さいスコープで高品質を担保してから拡大する方が効率的である
- フェーズごとにAutomated Reasoningの適用範囲を明確にできる

## リスクと対応
- リスク: SQLite3の暗黙的な挙動（type affinity等）の再現が困難
- 対応（軽減）: Differential Testingで差異を早期検出する
