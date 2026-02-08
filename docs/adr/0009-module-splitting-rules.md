# ADR-0009: Module Splitting Rules

## Status
Accepted

## Context

`packages/core/src/root.zig` が6000行超に肥大化した。原因:

- 新機能を既存ファイルに追加するのが最も抵抗が少ないため、常にroot.zigに追加されてきた
- ファイルサイズの上限やモジュール分割のタイミングに関するルールが存在しなかった
- アーキテクチャ設計図がなく、どのコードをどこに置くかの指針がなかった

## Decision

### 3つのルール

1. **500行ルール**: 1ファイルが500行を超えたら、次の機能追加前に分割する
2. **2責務ルール**: 1ファイルに2つ以上の独立した責務が含まれたら分割する
3. **新機能は既存巨大ファイルに追加しない**: 500行超のファイルに新しいpub fnを追加するには、まず関連コードを別モジュールに抽出する

### 分割の判断基準

- **入力/出力の型**: 同じ型を受け取り同じ型を返す関数群は1モジュール
- **呼び出し方向**: AがBを呼ぶがBはAを呼ばないなら、Bは分離可能
- **テスト容易性**: モジュール単独でテストできることが理想

### core package target structure

```
root.zig         → Database struct, execute dispatch (<500 lines)
eval.zig         → Expression evaluation (evalExpr, scalar functions, date/time)
aggregate.zig    → Aggregate computation (computeAgg, GROUP BY)
window.zig       → Window functions
select.zig       → SELECT execution
join.zig         → JOIN execution
union_ops.zig    → UNION/INTERSECT/EXCEPT, ORDER BY sorting
cte.zig          → CTE handling
modify.zig       → INSERT/UPDATE/DELETE helpers, constraints
pragma.zig       → PRAGMA, sqlite_master
convert.zig      → Value type conversion, formatFloat
state.zig        → Projected state, transaction snapshots
```

### 検証方法

コミット前に `wc -l packages/*/src/*.zig` を実行し、500行超のファイルがないことを確認する。

## Consequences

- 新機能追加時に「どこに置くか」が明確になる
- レビュー時にファイルサイズで機械的にチェックできる
- 初期コスト: root.zig (6000行) と parser/root.zig (3200行) のリファクタリングが必要
- Zigの`@import`を使ったモジュール間参照パターンの確立が必要
