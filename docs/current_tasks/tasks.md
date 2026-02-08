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

### 結果
- ユニットテスト: 23/23 passing
- Differential Test: 32/32 passing

## Phase 6b: SQL機能充実（完了）

### Phase 6b-1: 比較演算子の式統合 + 複数SET UPDATE（完了）
- [x] BinOp enumに比較演算子追加（eq, ne, lt, le, gt, ge）
- [x] 式パーサー優先度階層を拡張（parseComparison層追加）
- [x] evalExprで比較結果を0/1で返す
- [x] Update構造体を複数SET対応に変更
- [x] parseUpdateでカンマ区切り複数SETをパース
- [x] Differential Test追加: 34_multi_set_update.sql, 35_comparison_expr.sql

### Phase 6b-2: CASE WHEN式 + LIKE演算子（完了）
- [x] トークナイザにCASE/WHEN/THEN/ELSE/END/LIKEキーワード追加
- [x] Expr AST に case_when バリアント追加
- [x] parsePrimary で CASE WHEN パーサー実装
- [x] evalExpr で CASE WHEN 評価ロジック実装
- [x] BinOp に LIKE 追加
- [x] CompOp に LIKE 追加
- [x] WHERE句で LIKE パターンマッチング実装
- [x] likeMatch ヘルパー関数実装（%, _, 大文字小文字非区別）
- [x] Differential Test追加: 36_case_when.sql, 37_like.sql

### 結果
- ユニットテスト: 23/23 passing
- Differential Test: **36/36 passing**（前回32 → 追加4件）

## Phase 6c: WHERE句の式ベース化（完了）

### 実施内容
- [x] BinOpにlogical_and, logical_or追加
- [x] UnaryOp enum追加（is_null, is_not_null）
- [x] Exprにunary_op, in_list, scalar_subqueryバリアント追加
- [x] parseExprにAND/OR（parseLogical層）追加
- [x] parseComparisonにIS NULL/IS NOT NULL/IN処理追加
- [x] parsePrimaryでスカラーサブクエリ(SELECT...)認識
- [x] Select/Delete/Updateにwhere_exprフィールド追加
- [x] parseDelete/parseUpdateをwhere_exprベースに変更
- [x] parseSelectをwhere_exprベースに変更（JOINあり時は旧WhereClause維持）
- [x] evalExprにAND/OR短絡評価、IS NULL/IS NOT NULL、IN、scalar_subquery追加
- [x] compareValuesOrderで integer vs text の数値比較対応（AVG等のfloat値に対応）
- [x] freeExprDeepでExprツリー全体（subquery_sql含む）を再帰的解放
- [x] execute()内でwhere_expr/result_exprs/set_columns/set_valuesの適切なdefer解放
- [x] Differential Test追加: 38_where_expr.sql, 39_where_is_null.sql

### 結果
- ユニットテスト: 23/23 passing（メモリリーク0）
- Differential Test: **38/38 passing**（前回36 → 追加2件）

## 予定

### Phase 6d: ストレージ抽象化 + B-Tree
- StorageBackend traitの定義
- B-Treeデータ構造の実装
- 行検索・走査の抽象化

### Phase 6e: ファイル永続化
- ページベースのファイルI/O
- SQLite3互換フォーマット

### Phase 6f: WAL
- Write-Ahead Logging
