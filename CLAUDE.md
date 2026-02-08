# sqlite0 - Project Intelligence

## Common Commands

```bash
# Build
zig build

# Run REPL
zig build run

# Unit tests
zig build test

# Differential tests against SQLite3
bash tests/differential/run.sh
```

## Project Structure

```
app/src/main.zig          # REPL executable
packages/tokenizer/src/   # Lexical analysis (TokenType, Tokenizer)
packages/parser/src/      # SQL parsing (Statement, Expr, Parser)
packages/core/src/        # Database engine (Database, Table, Value)
tests/differential/       # run.sh + cases/*.sql (SQLite3 comparison)
```

```
docs/ideas ... ここに開発アイデアを追加してください。定期的にリファインメントする。
docs/current_tasks ... ここで現在のタスクを管理してください。完了したタスクは削除する。
docs/adr ... Architectural Decision Recordsをここに保存する。ストック情報になるように継続的に更新する。フロー情報はmemoryに保存する。
docs/memory ... 開発中に得た知見やノウハウを蓄積していく場所です。
app ... 実行可能なパッケージ
packages ... ライブラリパッケージ
```

Module dependency: `app -> core -> parser -> tokenizer`

## Commit Conventions

```
<type>: <short description in English>
```

Types: `feat`, `fix`, `improve`, `refactor`, `docs`, `chore`

## Module Splitting Rules

### Trigger: When to split

1. **500行ルール**: 1ファイルが500行を超えたら、次の機能追加前に分割する。既存の500行超ファイルは段階的に分割する。
2. **2責務ルール**: 1ファイルに2つ以上の独立した責務（例: 式評価とJOIN実行）が含まれたら分割する。
3. **新機能は既存巨大ファイルに追加しない**: 500行超のファイルに新しいpub fnを追加する場合、まず関連コードを別モジュールに抽出してから追加する。

### Boundary: How to decide where to split

- **入力/出力の型で分ける**: 同じ型を受け取り同じ型を返す関数群は1モジュールにまとめる
- **呼び出し方向で分ける**: AがBを呼ぶがBはAを呼ばないなら、Bは別モジュールにできる
- **テスト容易性で分ける**: 単体テストを書くとき、モジュール単独でテストできるのが理想

### Structure: core package modules

```
packages/core/src/
  root.zig            # Database struct, pub fn execute(), dispatch only (<500 lines)
  eval.zig            # evalExpr, evalScalarFunc, date/time functions
  aggregate.zig       # computeAgg, executeGroupByAggregate, executeAggregate
  window.zig          # evaluateWindowFunctions, computeWindowAggregates
  select.zig          # executeSelect, executeTablelessSelect, evaluateExprSelect
  join.zig            # executeJoin
  union_ops.zig       # executeUnion, sortRowsByOrderBy, set operations
  cte.zig             # executeWithCTE, recursive CTE
  modify.zig          # INSERT/UPDATE/DELETE helpers, RETURNING, constraint validation
  pragma.zig          # executePragma, executeSqliteMaster
  convert.zig         # valueToText, valueToF64, formatFloat, valueToBool
  state.zig           # freeProjected, ProjectedState, transaction snapshots
  table.zig           # Table struct (existing)
  value.zig           # Value, Row, Column (existing)
  row_storage.zig     # RowStorage vtable (existing)
  array_list_storage.zig # ArrayListStorage (existing)
```

### Verification

新機能をコミットする前に `wc -l packages/*/src/*.zig` を実行し、500行超のファイルがないことを確認する。

## Current Phase

Check `docs/current_tasks/tasks.md` for active work. Development follows phased approach documented in ADRs.
