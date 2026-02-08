# Phase 6c: WHERE句の式ベース化 実装ノート

## 式パーサーの優先度階層（更新後）

```
parseExpr() → parseLogical()
  parseLogical(): AND, OR（最低優先度）
    parseComparison(): =, !=, <, <=, >, >=, LIKE, IS NULL, IS NOT NULL, IN
      parseAddSub(): +, -, ||
        parseMulDiv(): *, /
          parsePrimary(): リテラル, カラム参照, 集約関数, CASE WHEN, (SELECT...), 括弧
```

## Expr AST拡張

Phase 6c以降のExprバリアント:
```
integer_literal, string_literal, column_ref, null_literal, star,
binary_op, aggregate, case_when,
unary_op,       -- IS NULL / IS NOT NULL
in_list,        -- col IN (SELECT ...)
scalar_subquery -- (SELECT ...)
```

## WHERE評価の切り替え

- SELECT (JOINなし)、DELETE、UPDATE: `where_expr` (Expr) ベース → `evalExpr` で評価
- SELECT (JOINあり): 旧 `WhereClause` ベース → テーブル修飾名の解決が必要なため
- `valueToBool`: SQLite3互換 (0/NULL→false, その他→true)

## AND/OR短絡評価

`evalExpr`内のlogical_and/logical_or処理:
- AND: 左がfalseなら右を評価せず0を返す
- OR: 左がtrueなら右を評価せず1を返す

## compareValuesOrderの改善

integer vs text比較時、textを数値としてパースし成功すればfloat比較する。
これによりAVG()の結果（text "150.0"）と整数カラムの比較が正しく動作する。

## メモリ管理

- `freeExprDeep`: Database側のExprツリー再帰解放（subquery_sqlのallocバッファ含む）
- execute()の各Statement分岐でdefer解放:
  - select_stmt: result_exprs（各Expr）+ where_expr
  - delete: where_expr
  - update: set_columns + set_values + where_expr
