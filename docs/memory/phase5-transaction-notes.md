# Phase 5: トランザクション実装ノート

## インメモリスナップショット方式

BEGIN時にデータベース全体のディープコピーを取得し、ROLLBACK時に復元する方式を採用した。

### ディープコピーの注意点
- `Value.text` はポインタ参照のため、`dupeStr` で必ず新しいメモリにコピーする
- `Column.name` も同様にコピーが必要
- `Column.col_type` は定数文字列 ("INTEGER", "TEXT") のため、コピー不要
- `Table.rows` は `ArrayList` だが、スナップショットでは `[]const Row` スライスに変換して保持

### freeSnapshot の安全な解放順序
1. 行ごとにテキスト値をfree
2. 行のvaluesスライスをfree
3. rowsスライスをfree
4. カラム名をfree
5. columnsスライスをfree
6. テーブル名をfree
7. snapshotsスライスをfree

### SQLite3互換性
- `BEGIN;` と `BEGIN TRANSACTION;` の両方をサポート（TRANSACTION は省略可能）
- `COMMIT;` と `COMMIT TRANSACTION;` も同様
- `ROLLBACK;` と `ROLLBACK TRANSACTION;` も同様
- ネストされたトランザクションはエラーを返す（SQLite3互換）

## リファクタリング: executeSelect / projectColumns

`execute` 関数内の `select_stmt` 処理（約150行）を以下の2メソッドに分割した:
- `executeSelect`: WHERE → 集約判定 → ORDER BY → DISTINCT → LIMIT/OFFSET → 投影
- `projectColumns`: カラムインデックス解決 → 値コピー → DISTINCT（投影後）

これにより `execute` 関数の可読性が向上し、B-Tree導入時の変更範囲を限定できる。
