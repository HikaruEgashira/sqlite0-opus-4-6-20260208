# Phase 6a: Expression Evaluation 実装ノート

## 設計

### Expr AST型
```zig
Expr = union(enum) {
    integer_literal: i64,
    string_literal: []const u8,
    column_ref: []const u8,
    null_literal: void,
    star: void,
    binary_op: struct { op: BinOp, left: *const Expr, right: *const Expr },
    aggregate: struct { func: AggFunc, arg: *const Expr },
}
```

ポインタベースの再帰構造。Zigでは`union`内に自己参照を直接持てないため、`*const Expr`を使用。

### 演算子優先度
再帰下降パーサーで実現:
- `parseExpr()`: `+`, `-`, `||`（低優先度）
- `parseMulDiv()`: `*`, `/`（高優先度）
- `parsePrimary()`: リテラル、カラム参照、集約関数、括弧

### 後方互換性
既存の`SelectExpr`型に`.expr`バリアントを追加。単純なカラム参照と集約関数は既存のバリアント(`.column`, `.aggregate`)として保持し、複合式のみ`.expr`を使用。これにより既存のCore処理パスが変更なしで動作。

## メモリ管理

### Exprノードの割り当て
`allocExpr()`: `allocator.create(Expr)`で個別ノードをヒープ上に割り当て。

### Exprノードの解放
`freeExpr()`: 再帰的にツリーを走査し、各ノードを`allocator.destroy()`で解放。
`freeExprSlice()`: result_exprsスライス全体を解放。

### evalExpr内のテキスト管理
- `column_ref`: テキスト値は`dupeStr`でコピー（呼び出し元がfreeする責務）
- `string_literal`: `dupeStr`でコピー
- `binary_op`: 左右の値を評価後、結果を生成してから左右をfree
- `concat`: 両側を文字列に変換→結合→元をfree

## NULL伝播
SQL標準に従い、算術演算でNULLが含まれるとNULLを返す。
`concat`も同様（SQLite3互換）。

## 検証結果
- ユニットテスト: 23/23 通過、メモリリーク0
- Differential Test: 32/32 通過
- 新規テスト: 31_arithmetic_expr, 32_string_concat, 33_expr_with_null
