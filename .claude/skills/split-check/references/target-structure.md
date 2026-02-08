# Target Module Structure (ADR-0009)

## packages/core/src/

```
root.zig             → Database struct, pub fn execute(), dispatch only (<500 lines)
eval.zig             → evalExpr, evalScalarFunc, evalGroupExpr, date/time functions
aggregate.zig        → computeAgg, executeGroupByAggregate, executeAggregate, collectAggTexts
window.zig           → evaluateWindowFunctions, computeWindowAggregates, computeWindowValueFunctions
select.zig           → executeSelect, executeTablelessSelect, evaluateExprSelect, projectColumns
join.zig             → executeJoin
union_ops.zig        → executeUnion, executeSetOperands, sortRowsByOrderBy, removeDuplicateRows
cte.zig              → executeWithCTE, executeNonRecursiveCTE, executeRecursiveCTE, createCTETable
modify.zig           → expandInsertValues, hasPkConflict, hasUniqueConflict, validateCheckConstraints, replaceRow, executeUpsertRow, executeInsertSelect, buildReturningResult
pragma.zig           → executePragma, executeSqliteMaster
convert.zig          → valueToText, valueToI64, valueToF64, isFloatValue, formatFloat, valueToBool
state.zig            → freeProjected, ProjectedState, saveProjectedState, restoreProjectedState, freeSnapshot, takeSnapshot, restoreSnapshot
table.zig            → Table struct (existing)
value.zig            → Value, Row, Column (existing)
row_storage.zig      → RowStorage vtable (existing)
array_list_storage.zig → ArrayListStorage (existing)
```

## packages/parser/src/

```
root.zig             → Parser struct, types, parse() dispatch (<500 lines)
select_parser.zig    → parseSelect, parseOrderByClause, parseSelectExpr
expr_parser.zig      → parseExpr, parsePrimary, parseComparison, etc.
insert_parser.zig    → parseInsert, parseReplace, parseValues
update_parser.zig    → parseUpdate
other_parser.zig     → parseCreate, parseDrop, parseAlter, parsePragma, parseWith
```

## Zigモジュール分割パターン

Databaseのメソッドを別ファイルに分割する場合:

```zig
// eval.zig
const root = @import("root.zig");
const Database = root.Database;
const Value = @import("value.zig").Value;

pub fn evalExpr(db: *Database, expr: *const Expr, tbl: *const Table, row: Row) !Value {
    // ...
}
```

```zig
// root.zig
const eval = @import("eval.zig");

// Database.execute() 内で:
const val = try eval.evalExpr(self, expr, tbl, row);
```
