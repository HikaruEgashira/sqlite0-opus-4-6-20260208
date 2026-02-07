# 現在のタスク

## Phase 5.5: 品質強化（完了）

### 実施内容
- [x] エッジケーステストの拡充（5つの新規テストケースを追加し検証）
  - 25_empty_table.sql - 空テーブルへの集約関数
  - 26_order_by_null.sql - NULLのソート順
  - 27_group_by_null.sql - NULLのグループ化
  - 28_sequential_ops.sql - 連続的なDML操作
  - 30_null_compare.sql - NULL比較のセマンティクス
- [x] バグ修正: SUM/AVG が空テーブルで0を返す問題をNULLに修正
- [x] テスト基盤改善: テストランナーがNULL出力とプロンプト重複を正確に処理

### 結果
- テスト統計: 29/29 passing（フェーズ1～5までの全機能）
- バグ発見・修正: 3件（SUM/AVG, テストフィルタリング）

## 予定

### Phase 6: B-Treeストレージ + ファイルフォーマット
- ページベースのストレージ設計
- SQLite3互換のファイルフォーマット
- WAL（Write-Ahead Logging）

### 将来: Expression Evaluation（段階的実装）
- Tokenizer: 算術演算子追加 ✓
- Parser: Expr AST型定義 ✓
- Parser: 式パーサー実装（保留）
- Core: 式評価エンジン（保留）

理由: Expression Evaluation は複雑で、ストレージ基盤の確立を優先したい。
現在のSQLサポートは十分に機能している（COUNT*, GROUP BY, JOIN, Aggregate, Subquery等）。
