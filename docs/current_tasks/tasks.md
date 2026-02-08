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

### Phase 6d-9: SQL機能充実（完了）
- [x] LEFT/RIGHT OUTER JOIN構文
- [x] ビット演算子（&, |, ~, <<, >>）
- [x] AUTOINCREMENT（INTEGER PRIMARY KEY）
- [x] CHECK制約（CREATE TABLE + INSERT検証）
- [x] 浮動小数点リテラル（3.14, -2.5）
- [x] REAL列型サポート
- [x] 浮動小数点演算（+, -, *, /）
- [x] 負数パースのメモリリーク修正
- Differential Test: **102/102 passing**

### Phase 6d-10: SQL機能充実（完了）
- [x] <>演算子
- [x] Date/Time関数（date, time, datetime, strftime）
- [x] FULL OUTER JOIN
- [x] Window関数（ROW_NUMBER, RANK, DENSE_RANK）
- [x] NATURAL JOIN
- [x] CREATE TABLE AS SELECT
- [x] UPSERT (INSERT ... ON CONFLICT DO NOTHING/UPDATE)
- [x] Common Table Expressions (WITH/CTE)
- [x] Simple CASE expression (CASE expr WHEN val ...)
- [x] INSERT INTO ... DEFAULT VALUES
- [x] NULLS FIRST/LAST in ORDER BY
- Differential Test: **113/113 passing**

### Phase 6d-11: SQL機能充実（完了）
- [x] Window集約関数（SUM/AVG/COUNT/MIN/MAX/TOTAL OVER）
- [x] PARTITION BY テスト追加
- [x] LIKE() / GLOB() スカラー関数
- [x] LAG / LEAD Window関数
- [x] NTILE Window関数
- [x] FIRST_VALUE / LAST_VALUE Window関数
- [x] クォート識別子（"name", `name`）
- [x] WITH RECURSIVE（再帰CTE）
- [x] || 連結演算子テスト追加
- [x] IS NULL / IS NOT NULL テスト追加
- Differential Test: **123/123 passing**

### Phase 6d-12: バグ修正・品質向上（完了）
- [x] SQLite互換float format (%.15g: 15有効桁+丸め)
- [x] SUBSTRの負のインデックス・負のlength対応
- [x] GROUP BY式サポート（GROUP BY UPPER(name), id % 2）
- [x] GROUP BY結果のORDER BY（エイリアス解決付き）
- [x] JOIN結果の集約関数（COUNT/SUM/MAX + JOIN）
- [x] MIN/MAX集約関数のNULLスキップ
- [x] LIMIT/OFFSETのGROUP BY集約への適用
- [x] 集約関数キーワードをidentifierとしても使用可能（ORDER BY total等）
- [x] テーブルなしSELECTのORDER BY/LIMIT/OFFSET対応
- [x] 差分テスト追加: float format, SUBSTR負数, GROUP BY式, NULL処理, 型変換, 空テーブル, ネスト関数, CASE式, JOIN集約, LIMIT集約, BETWEEN, 文字列操作
- Differential Test: **140/140 passing**

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
