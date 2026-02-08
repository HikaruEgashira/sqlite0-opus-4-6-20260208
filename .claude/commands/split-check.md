# Module Split Check

プロジェクト全体のファイルサイズを検査し、モジュール分割ルール（ADR-0009）への違反を報告してください。

## 手順

1. `wc -l packages/*/src/*.zig app/src/*.zig` を実行してファイル行数を取得
2. 以下の基準で判定:
   - **500行超**: 分割が必要（違反）
   - **300-500行**: 注意（次の機能追加時に分割を検討）
   - **300行以下**: 正常
3. 違反ファイルに対して、ADR-0009のモジュール構成に基づいた具体的な分割案を提示
4. 分割案には以下を含めること:
   - 抽出対象の関数名と行範囲
   - 新モジュール名
   - 依存関係（どのモジュールがどのモジュールを参照するか）

## ADR-0009 target structure (packages/core/src/)

```
root.zig         → Database struct, execute dispatch (<500 lines)
eval.zig         → evalExpr, evalScalarFunc, date/time functions
aggregate.zig    → computeAgg, executeGroupByAggregate
window.zig       → evaluateWindowFunctions, computeWindowAggregates
select.zig       → executeSelect, executeTablelessSelect, evaluateExprSelect
join.zig         → executeJoin
union_ops.zig    → executeUnion, sortRowsByOrderBy
cte.zig          → executeWithCTE, recursive CTE
modify.zig       → INSERT/UPDATE/DELETE helpers, constraints, RETURNING
pragma.zig       → executePragma, executeSqliteMaster
convert.zig      → valueToText, valueToF64, formatFloat, valueToBool
state.zig        → freeProjected, ProjectedState, transaction snapshots
```

## 出力形式

```
## File Size Report

| File | Lines | Status |
|------|-------|--------|
| ... | ... | OK / WARNING / VIOLATION |

## Violations

### <filename> (<lines> lines)
- 現在の責務: ...
- 分割案:
  1. <new_module>.zig に <function_list> を抽出 (約N行)
  2. ...
- 分割後の見込みサイズ: ...
```
