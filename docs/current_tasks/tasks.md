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

### Phase 6d-13: SQL機能充実（完了）
- [x] CREATE VIEW / DROP VIEW（IF NOT EXISTS, IF EXISTS対応）
- [x] ビューのマテリアライズ（temp_cte_namesパターンでクリーンアップ）
- [x] FROM句のサブクエリ（導出テーブル）: SELECT * FROM (SELECT ...) alias
- [x] 集約クエリの projected_column_names 設定修正
- [x] FOREIGN KEY / REFERENCES 構文（パース対応）
- [x] UNIQUE制約構文（カラム・テーブルレベル）
- [x] 複合PRIMARY KEY構文（テーブルレベル）
- [x] テーブルレベルCHECK/FOREIGN KEY/UNIQUE/PRIMARY KEY制約パース
- [x] UNIQUE制約のINSERT時検証（INSERT OR IGNOREも対応）
- [x] 拡張カラム型サポート（VARCHAR, INT, BOOLEAN, BLOB, DECIMAL等）
- [x] .tables / .schema / .quit REPLコマンド
- Differential Test: **145/145 passing**

### Phase 6d-14: SQL機能充実（完了）
- [x] Window running/cumulative aggregates (SUM/AVG/COUNT/MIN/MAX/TOTAL OVER ORDER BY)
- [x] 暗黙的Window ORDER BYソート（PARTITION BY + ORDER BY）
- [x] Window関数expression cleanup（order_by/partition_byメモリリーク修正）
- [x] Math functions (CEIL, FLOOR, SQRT, POWER, LOG, LN, EXP, trig, PI, MOD)
- [x] RETURNING clause (INSERT/UPDATE/DELETE RETURNING */columns)
- [x] UNION/INTERSECT/EXCEPT結果のメモリリーク修正（projected_values追跡）
- Differential Test: **148/148 passing**

### Phase 6d-15: SQL機能充実・バグ修正（完了）
- [x] ORDER BY column position numbers (ORDER BY 2 DESC)
- [x] GROUP BY column position numbers (GROUP BY 1, 2)
- [x] SUM/AVG/TOTAL aggregate on float values (valueToF64)
- [x] MIN/MAX text value duplication (prevent table data corruption)
- [x] Optional column types in CREATE TABLE (e.g., CREATE TABLE t (val))
- [x] Expressions in INSERT VALUES (string concat, arithmetic, functions)
- Differential Test: **154/154 passing**

### Phase 6d-16: SQL機能充実・バグ修正（完了）
- [x] 集約式のSELECT/ORDER BY対応（SUM(val)*2, ROUND(AVG(val)), COUNT(*)||' items'等）
- [x] evalGroupExprのscalar_func/case_when/unary_op/concat対応
- [x] evalScalarFuncValues（値ベースのスカラー関数評価）
- [x] 相関サブクエリ（outer_contextメカニズム）
- [x] EXISTS/NOT EXISTS/IN相関サブクエリ
- [x] qualified_ref（e.salary）の集約関数対応
- [x] exprAsAggregateのqualified_ref対応（"table.column"形式）
- [x] LEFT JOIN COUNT(qualified_ref)のNULL行カウント修正
- [x] AliasOffset追跡によるJOIN集約の正しいカラム解決
- Differential Test: **158/158 passing**

### Phase 6d-17: SQL機能充実・REPL改善（完了）
- [x] JOIN DISTINCT / LIMIT / OFFSET対応
- [x] マルチラインSQL対応（REPL: `;`まで継続入力、`...>`プロンプト）
- [x] JOINクエリでの式評価（SELECT e.salary * 12, UPPER(d.name)）
- [x] evalExprのqualified_refでjoin_alias_offsets対応（重複カラム名解決）
- [x] 集約クエリORDER BYのqualified_ref/column_ref式解決
- [x] セルフジョイン対応テスト
- [x] 差分テスト追加（161-170: サブクエリ、COALESCE、マルチJOIN、Window、CTE、UNION、JOIN式、セルフジョイン）
- Differential Test: **169/169 passing**

### Phase 6d-18: スキーマ情報・構文改善（完了）
- [x] PRAGMA table_info(table) — カラムメタデータ返却（cid, name, type, notnull, dflt_value, pk）
- [x] PRAGMA table_list — テーブル一覧返却
- [x] sqlite_master / sqlite_schema 仮想テーブル（SELECT * FROM sqlite_master）
- [x] LIMIT offset, count 構文（LIMIT 2, 3 = skip 2 take 3）
- [x] projected_textsによるメモリ追跡
- Differential Test: **172/172 passing**

### Phase 6d-3: インメモリB+Tree実装（完了）
- [x] Row構造体にrowid追加（デフォルト0で後方互換）
- [x] Table.insertRowでrowid自動割当（next_rowid管理）
- [x] B+TreeStorage実装（ORDER=4、リーフリンクリスト、マテリアライズキャッシュ）
- [x] insert/delete/search/rangeSearch O(log N)操作
- [x] scanMut write-back（ALTER TABLE ADD COLUMN対応）
- [x] tables.get()→getPtr()修正（BTreeStorageキャッシュリーク防止）
- [x] storage().append()全箇所にrowid付与（state/modify/select修正）
- [x] rowid疑似カラム（SELECT rowid, WHERE rowid=N）
- [x] TableSnapshotにnext_rowid保存・復元
- Differential Test: **173/173 passing**

### Phase 6d-19: SQL関数・バグ修正（完了）
- [x] QUOTE() スカラー関数（テキストのクォーティング、エスケープ）
- [x] TYPEOF() が float-like テキストに 'real' を返すように修正
- [x] last_insert_rowid() / changes() / total_changes() 関数
- [x] CAST(float AS INTEGER) の切り捨て修正（3.14→3）
- [x] キーワード（key, value等）をカラム名に使用可能に
- [x] 差分テスト追加: quote/typeof, changes/rowid, cast/float, coalesce/null, upsert, 式, rowid操作
- Differential Test: **180/180 passing**

### 次のステップ

### Phase 6e: ファイル永続化
- ページベースのファイルI/O
- SQLite3互換フォーマット

### Phase 6f: WAL
- Write-Ahead Logging
