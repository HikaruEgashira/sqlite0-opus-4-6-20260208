# Phase 6b: SQL機能充実 実装ノート

## 式パーサーの優先度階層

```
parseExpr() → parseComparison()
  parseComparison(): =, !=, <, <=, >, >=, LIKE（最低優先度）
    parseAddSub(): +, -, ||
      parseMulDiv(): *, /
        parsePrimary(): リテラル, カラム参照, 集約関数, CASE WHEN, 括弧
```

## BinOp拡張

Phase 6b以降のBinOp:
```
add, sub, mul, div, concat, eq, ne, lt, le, gt, ge, like
```

比較演算子はevalExprで整数0/1を返す（SQLite3互換）。

## CASE WHEN実装

Expr ASTのcase_whenバリアント:
- `conditions: []const *const Expr` - WHEN条件（比較式）
- `results: []const *const Expr` - THEN値
- `else_result: ?*const Expr` - ELSE値（オプション）

evalExprでの評価: 各条件を順次評価、非0/非NULLが真、マッチしたTHEN値を返す。

### 制限事項
CASE WHEN式はSELECT式リスト内では使用不可（WHERE句からのみ間接的に利用可能）。理由: SELECT句のパーサーがExprベースではなくSelectExprベースであるため。Phase 6cでWHERE句の式ベース化と合わせて対応予定。

## LIKE演算子

### WHERE CompOpとして
`WHERE col LIKE 'pattern'` 形式。CompOpに`.like`を追加。

### BinOpとして
式中で `col LIKE 'pattern'` も使用可能（parseComparisonでLIKEをBinOp.likeとしてパース）。

### パターンマッチ
`likeMatch()` 関数（再帰実装）:
- `%`: 0文字以上の任意の文字列
- `_`: 任意の1文字
- 大文字小文字非区別（`std.ascii.toLower`）

## 複数SET UPDATE

Update構造体を`set_column/set_value`（単一）から`set_columns/set_values`（スライス）に変更。パーサーでカンマ区切りの複数SETをArrayListで収集し、toOwnedSliceで変換。

## Zig 0.15.2 API注意点

- `ArrayList.init(allocator)` → `ArrayList: .{}` (0.15.2では初期化時にallocatorを渡さない)
- `list.append(item)` → `list.append(allocator, item)` (操作時にallocatorを渡す)
- `list.deinit()` → `list.deinit(allocator)`
- `list.toOwnedSlice()` → `list.toOwnedSlice(allocator)`
