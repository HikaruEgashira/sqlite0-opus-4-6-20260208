# 現在のタスク

## Phase 6a: Expression Evaluation（完了）

### 実施内容
- [x] Parser: Expr AST型の定義（リテラル、カラム参照、二項演算、集約関数）
- [x] Parser: 再帰下降式パーサーの実装（優先度: *, / > +, -, ||）
- [x] Parser: SelectExprにExprバリアント追加（後方互換維持）
- [x] Core: 式評価エンジン（evalExpr）の実装
- [x] Core: evaluateExprSelectで式評価を統合
- [x] メモリ管理: Expr再帰的free、evalExpr内テキスト管理
- [x] Differential Test: 3つの新規テストケース追加
  - 31_arithmetic_expr.sql - 算術式（+, -, *, 優先度, 括弧）
  - 32_string_concat.sql - 文字列結合（||）
  - 33_expr_with_null.sql - NULL伝播

### 結果
- ユニットテスト: 23/23 passing
- Differential Test: 32/32 passing

## 予定

### Phase 6b: ストレージ抽象化 + B-Tree
- StorageBackend traitの定義
- B-Treeデータ構造の実装

### Phase 6c: ファイル永続化
- ページベースのファイルI/O
- SQLite3互換フォーマット

### Phase 6d: WAL
- Write-Ahead Logging
