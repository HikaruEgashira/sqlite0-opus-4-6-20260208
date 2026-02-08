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
docs/ideas ... 未着手のアイデアのみ保持。実装済みは削除する。
docs/current_tasks ... 進行中タスクのみ保持。完了したタスクは削除する。
docs/adr ... 現在有効な設計判断のみ。後続ADRで置き換えられたものは統合・削除する。
docs/memory ... 現在のコードに関連する知見のみ。実装済みで自明になったものは削除する。
app ... 実行可能なパッケージ
packages ... ライブラリパッケージ
```

## Content Lifecycle Rules

各ドキュメントは追加だけでなく、定期的に削除・整理する。

### 削除トリガー
1. **実装完了**: `docs/ideas` の完了済みアイテムを削除する
2. **ADR統合**: 後続のADRで意思決定が更新された場合、古いADRの内容を新しいADRに統合して古いものを削除する
3. **知見の陳腐化**: `docs/memory` の内容がコードから自明になった場合、または対象コードが削除された場合は削除する
4. **スタブ禁止**: 1-2行の中身のないファイルは作成しない。具体的な内容がある場合のみファイルを作成する

### 実施タイミング
- 新しいセッション開始時に `docs/` 配下を確認し、不要なファイルを削除する
- コミット前に不要なコンテンツがないか確認する

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
