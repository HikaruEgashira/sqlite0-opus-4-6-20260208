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
- [x] カラムエイリアス（AS）
- [x] 複数ORDER BYカラム
- [x] UPDATE SET col = expr（式による更新）
- [x] Differential Test: **61/61 passing**

### Phase 6d-5: SQL機能充実（完了）
- [x] IIF関数
- [x] CAST式
- [x] REPLACE文
- [x] SUBSTR / INSTR / REPLACE関数
- [x] GROUP_CONCAT集約関数（カスタムセパレーター対応）
- [x] INSERT OR IGNORE / INSERT OR REPLACE
- [x] HEX / UNICODE / CHAR / ZEROBLOB / PRINTF関数
- [x] UPDATE SET col = expr（式による更新）
- [x] Differential Test: **68/68 passing**

### Phase 6d-6: SQL機能充実（完了）
- [x] COUNT(DISTINCT col)
- [x] HAVING式ベースフィルター
- [x] IN (val1, val2, ...) 値リスト
- [x] ORDER BY式サポート（ABS(val), val+1など）
- [x] テーブルエイリアス（FROM t1 AS a, JOIN t2 AS b）
- [x] CROSS JOIN / カンマ区切りFROM
- [x] Differential Test: **76/76 passing**

### Phase 6d-7: SQL機能充実（完了）
- [x] JOINのWHERE式統一（WhereClause→Expr）
- [x] DELETE式ベースWHERE
- [x] LTRIM / RTRIM関数
- [x] 複数カラムGROUP BY
- [x] 複数JOIN対応（3テーブル以上）
- [x] HAVING式ベース化（evalGroupExpr）
- [x] COALESCE式テスト追加
- Differential Test: **83/83 passing**

### Phase 6d-8: SQL機能充実（完了）
- [x] ROUND関数（SQLite互換float出力）
- [x] IFNULL関数
- [x] RANDOM関数
- [x] TOTAL集約関数（空集合で0.0返却）
- [x] EXISTS / NOT EXISTS サブクエリ
- [x] SUM(DISTINCT), AVG(DISTINCT), GROUP_CONCAT(DISTINCT)
- [x] RIGHT JOIN
- [x] DEFAULT値サポート（CREATE TABLE + INSERT列リスト）
- [x] キーワードをエイリアスとして使用可能（expectAlias）
- [x] ネスト関数呼び出しテスト
- Differential Test: **91/91 passing**

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
