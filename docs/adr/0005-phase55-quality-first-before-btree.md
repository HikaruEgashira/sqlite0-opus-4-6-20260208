# ADR-0005: Phase 5.5 品質強化とB-Tree前の準備

## ステータス
採用

## コンテキスト

Phase 5（トランザクション実装）が完了し、次に Phase 6（B-Tree ストレージ）を計画していた。しかし B-Tree は大規模なアーキテクチャ変更を伴う（メモリベース → ファイルベース、ページ管理、インデックス構造の再実装）。導入前に現在の SQL 実行パイプラインの品質を十分に検証すべきと判断した。

現状：
- ユニットテストとdifferentialテストで24ケース通過
- エッジケーステストが不足（NULL処理、空テーブル、複合操作）
- SUM/AVG のバグが検出可能な状態

## 決定

Phase 5 と Phase 6 の間に **Phase 5.5「品質強化」フェーズを実施**する。以下の優先度で実施：

1. **エッジケーステストの追加と実行** - NULL処理、空テーブル、複合DML操作
2. **バグ修正** - テスト結果に基づいて発見されたバグを修正
3. **テスト基盤の改善** - テストランナーの堅牢性向上
4. **Expression Evaluation の準備**（オプション）- Tokenizer の拡張のみ

## 理由

### テスト駆動による品質保証（Critical）
- B-Tree 導入時に既存の SQL 機能が壊れるリスクが高い
- エッジケーステストが充実していれば、regression の検出が容易
- Differential Testing により SQLite3 との互換性を常に確認できる

### バグの早期発見（High）
- SUM/AVG の NULL 処理に問題があることをテストで発見
- Phase 5.5 で修正することで、B-Tree 実装時に引きずることがない
- テストケースの追加により、将来のバグ検出も容易になる

### Expression Evaluation は複雑（Medium）
- 完全な実装には parser の大幅な書き換えが必要
- 現在の SQL サポート（GROUP BY, JOIN, Aggregate, Subquery）で大部分のクエリに対応可能
- B-Tree ストレージの確立を優先する方が戦略的に有利

## リスク対応

| リスク | 対応戦略 |
|--------|----------|
| Phase 5.5 が長期化する | タイムボックス：1-2 マイルストーン程度で完了 |
| Expression Evaluation の需要が高まる | 別の ADR で段階的実装計画を立てる |
| テスト量の増加による保守負荷 | Differential Testing で SQLite3 と同期をとり続ける |

## 結果

### Phase 5.5 実施結果

**テストケース追加（5件）:**
- 25_empty_table.sql - 空テーブルに対する集約関数
- 26_order_by_null.sql - NULL 値のソート順確認
- 27_group_by_null.sql - NULL 値のグループ化
- 28_sequential_ops.sql - 連続的な DML 操作（INSERT → UPDATE → DELETE）
- 30_null_compare.sql - NULL 比較のセマンティクス（IS NULL vs =）

**バグ発見と修正:**
1. **SUM on empty table returns 0 instead of NULL**
   - 影響: 集約関数の精度
   - 修正: has_value フラグで追跡

2. **AVG on empty table returns 0 instead of NULL**
   - 影響: 平均値計算の正確性
   - 修正: cnt == 0 チェックを NULL に変更

3. **Test runner filters NULL output incorrectly**
   - 影響: テスト結果の正確性
   - 修正: sed の繰り返し置換と適切な空行処理

**テスト統計:**
- 開始: 24/24 passing
- 終了: 29/29 passing（新規5ケース全て通過）
- バグ修正: 3 件

## 意思決定

### Expression Evaluation の判断

**判定: Phase 6 以降に段階的実装**

理由：
- Tokenizer の演算子追加のみ実施（既に完了）
- Parser の Expr 型定義は定義済み（段階的実装に備える）
- 完全な式パーサー（優先度制御、括弧対応）は Phase 6 の後に実装可能

### 次のマイルストーン

次は Phase 6「B-Tree ストレージ」に移行：
- ページベースのメモリ管理
- SQLite3 互換ファイルフォーマット（Page header, Cell array, Freelist）
- ランダムアクセス可能な永続ストレージ

## ADR との参照

関連する ADR：
- 0002: Incremental SQL Compatibility - Phase 6 の計画に参照される
- 0003: Differential Testing Strategy - Phase 5.5 のテスト方法論の根拠
- 0004: Phase 5 Transaction - トランザクション基盤の上での品質検証
