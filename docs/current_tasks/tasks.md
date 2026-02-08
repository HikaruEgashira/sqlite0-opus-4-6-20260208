# 現在のタスク

## Phase 6d: ストレージ抽象化 + B-Tree

### Phase 6d-2: RowStorage抽象化レイヤー（完了）
- [x] RowStorage vtableインターフェース定義
- [x] ArrayListStorage バックエンド実装
- [x] Table.zigを ArrayListStorage 使用に修正
- [x] root.zigの20個アクセスサイトをstorage()経由に変更

### Phase 6d-4: SQL機能充実（完了）
- [x] INTERSECT / EXCEPT 実装（SetOp enum、パーサー・実行エンジン拡張）
- [x] GLOB演算子（case-sensitiveパターンマッチ: *, ?）
- [x] BETWEEN / NOT演算子 / 多行INSERT / 単項マイナス / 剰余 / IF EXISTS
- [x] 組込み関数: ABS, LENGTH, UPPER, LOWER, TRIM, TYPEOF, COALESCE, NULLIF
- [x] MAX/MIN スカラー関数（複数引数対応）
- [x] テーブルなしSELECT（FROM句なし式評価）
- [x] Differential Test: **58/58 passing**

### 次のステップ

### Phase 6d-3: インメモリB-Tree実装（予定）
- rowid（オートインクリメント）対応
- B+TreeStorageバックエンド実装
- 範囲検索機能

### Phase 6e: ファイル永続化
- ページベースのファイルI/O
- SQLite3互換フォーマット

### Phase 6f: WAL
- Write-Ahead Logging
