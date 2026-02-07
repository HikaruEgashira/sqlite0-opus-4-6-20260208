# ADR-0006: Phase 6分割とExpression Evaluation優先

## ステータス
採用

## コンテキスト
Phase 5.5完了後、Phase 6として「B-Tree + ファイルフォーマット + WAL」を一括実装する計画がある。
しかし、このスコープは1フェーズとしては過大であり、失敗リスクが高い。

一方、Expression Evaluation（`SELECT a + 1`, `WHERE x * 2 > 10`）は既存アーキテクチャ上で実装可能であり、
Tokenizerに算術演算子（+, -, /, ||）が既に追加済みである。

## 決定
Phase 6を以下のように分割する:
- **Phase 6a**: Expression Evaluation（式評価エンジン）
- **Phase 6b**: ストレージ抽象化 + B-Tree
- **Phase 6c**: ファイル永続化
- **Phase 6d**: WAL

Phase 6aから着手する。

## 理由
- スコープ制御: 各フェーズを小さく保ち、品質を担保する
- リスク軽減: Expression Evaluationは既存コードへの破壊的変更が少ない
- 段階的価値: SQL互換性が即座に向上する（算術式、文字列結合等）
- 準備済み: Tokenizerに演算子トークンが既に存在する

## リスクと対応
- リスク: Expression Evaluationの式パーサーが複雑になる（優先度、括弧、単項演算子）
- 対応（軽減）: 最小限の演算子セット（+, -, *, /, ||）から開始し、段階的に拡張する
