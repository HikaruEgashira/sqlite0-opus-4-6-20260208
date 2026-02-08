# 開発アイデア

## SQLサポート拡張
- ~~WHERE句の実装（比較演算子: =, <, >, <=, >=, !=）~~ ✓
- ~~DELETE文~~ ✓
- ~~UPDATE文~~ ✓
- ~~DROP TABLE文~~ ✓
- ~~ORDER BY / GROUP BY~~ ✓
- ~~集約関数 (COUNT, SUM, AVG, MIN, MAX)~~ ✓
- ~~LIMIT / OFFSET~~ ✓
- ~~JOIN (INNER JOIN, LEFT JOIN)~~ ✓
- ~~WHERE句の拡張（AND/OR）~~ ✓
- ~~サブクエリ~~ ✓
- ~~HAVING句~~ ✓
- ~~DISTINCT~~ ✓
- ~~NULL値の適切なハンドリング~~ ✓
- ~~ALTER TABLE~~ ✓
- ~~Expression Evaluation（算術式、文字列結合、括弧）~~ ✓
- ~~INSERT INTO ... SELECT~~ ✓
- ~~CASE WHEN式~~ ✓
- ~~LIKE演算子~~ ✓ / ~~GLOB演算子~~ ✓
- ~~UNION / UNION ALL / INTERSECT / EXCEPT~~ ✓
- ~~複数カラムのUPDATE SET~~ ✓
- ~~比較演算子を式として使用~~ ✓
- ~~WHERE句の式ベース化（AND/ORを含む完全なExpr化）~~ ✓
- ~~NOT演算子（NOT expr, NOT LIKE, NOT IN, NOT BETWEEN）~~ ✓
- ~~BETWEEN演算子~~ ✓
- ~~多行INSERT VALUES~~ ✓
- ~~単項マイナス演算子~~ ✓
- ~~剰余演算子 (%)~~ ✓
- ~~SQLコメント (--)~~ ✓
- ~~CREATE TABLE IF NOT EXISTS / DROP TABLE IF EXISTS~~ ✓
- ~~COALESCE / NULLIF 関数~~ ✓
- ~~ABS / LENGTH / UPPER / LOWER / TRIM / TYPEOF 関数~~ ✓
- ~~テーブルなしSELECT（SELECT 1+2; SELECT ABS(-10);）~~ ✓
- ~~MAX / MIN スカラー関数（複数引数）~~ ✓
- ~~カラムエイリアス（AS）~~ ✓
- ~~IIF 関数~~ ✓
- ~~型キャスト（CAST）~~ ✓
- ~~REPLACE文~~ ✓
- ~~SUBSTR / INSTR / REPLACE関数~~ ✓
- ~~GROUP_CONCAT集約関数~~ ✓
- ~~INSERT OR IGNORE / INSERT OR REPLACE~~ ✓
- ~~HEX / UNICODE / CHAR / ZEROBLOB / PRINTF関数~~ ✓
- ~~UPDATE SET col = expr（式による更新）~~ ✓
- ~~複数ORDER BYカラム~~ ✓
- ~~COUNT(DISTINCT col)~~ ✓
- ~~IN (val1, val2, ...) 値リスト~~ ✓
- ~~ORDER BY式サポート~~ ✓
- ~~テーブルエイリアス~~ ✓
- ~~CROSS JOIN / カンマ区切りFROM~~ ✓
- ~~ROUND / IFNULL / RANDOM 関数~~ ✓
- ~~TOTAL集約関数~~ ✓
- ~~EXISTS / NOT EXISTS サブクエリ~~ ✓
- ~~SUM(DISTINCT) / AVG(DISTINCT) / GROUP_CONCAT(DISTINCT)~~ ✓
- ~~RIGHT JOIN~~ ✓
- ~~DEFAULT値（CREATE TABLE + INSERT列リスト）~~ ✓
- ~~ビット演算子 (&, |, ~, <<, >>)~~ ✓
- ~~CHECK制約~~ ✓
- ~~AUTOINCREMENT~~ ✓
- ~~浮動小数点リテラル・REAL列型~~ ✓
- ~~LEFT/RIGHT OUTER JOIN構文~~ ✓
- Date/Time関数 (datetime, date, strftime)
- FULL OUTER JOIN
- Window関数 (ROW_NUMBER, RANK, etc.)
- 外部キー (FOREIGN KEY)

## ストレージ
- B-Tree ベースのページ管理 ← Phase 6d
- SQLite3互換のファイルフォーマット ← Phase 6e
- WAL (Write-Ahead Logging) ← Phase 6f
- ~~トランザクション (BEGIN/COMMIT/ROLLBACK)~~ ✓

## 品質保証
- プロパティベーステスト（ランダムSQL生成 + Differential Testing）
- ファジング基盤の構築
- Automated Reasoning: メモリ安全性の形式検証（Zig comptime + 外部ツール）
- ベンチマーク: SQLite3との性能比較

## 開発体験
- エラーメッセージの改善
- .schema コマンド
- .tables コマンド
- EXPLAIN 相当の機能

## 将来構想
- VDBE (Virtual Database Engine) の実装
- SQLite3のテストスイート互換
- C API互換レイヤー
