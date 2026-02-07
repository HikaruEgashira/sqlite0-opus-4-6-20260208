# 開発アイデア

## SQLサポート拡張
- ~~WHERE句の実装（比較演算子: =, <, >, <=, >=, !=）~~ ✓
- ~~DELETE文~~ ✓
- ~~UPDATE文~~ ✓
- ~~DROP TABLE文~~ ✓
- ~~ORDER BY / GROUP BY~~ ✓
- ~~集約関数 (COUNT, SUM, AVG, MIN, MAX)~~ ✓
- ~~LIMIT / OFFSET~~ ✓
- JOIN (INNER JOIN, LEFT JOIN)
- WHERE句の拡張（AND/OR/括弧）
- サブクエリ
- HAVING句
- DISTINCT
- NULL値の適切なハンドリング
- ALTER TABLE

## ストレージ
- B-Tree ベースのページ管理
- SQLite3互換のファイルフォーマット
- WAL (Write-Ahead Logging)
- トランザクション (BEGIN/COMMIT/ROLLBACK)

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
