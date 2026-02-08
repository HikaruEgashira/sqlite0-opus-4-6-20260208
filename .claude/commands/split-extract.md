# Module Split Extract

指定されたファイルからモジュールを抽出し、分割を実行してください。

## 引数

$ARGUMENTS: 抽出元ファイルパス（例: packages/core/src/root.zig）

## 手順

1. 対象ファイルを読み、関数一覧と行範囲を把握する
2. ADR-0009のtarget structureに基づき、抽出すべきモジュールを特定する
3. **最も依存が少ないモジュールから順に**1つずつ抽出する（一度に全部やらない）
4. 抽出手順:
   a. 新ファイルを作成し、対象関数を移動
   b. 元ファイルで `@import` と関数呼び出しを修正
   c. `zig build` でコンパイルが通ることを確認
   d. `zig build test` でユニットテストが通ることを確認
   e. `bash tests/differential/run.sh` で差分テストが通ることを確認
5. 全テストが通ったらコミットする（1モジュール = 1コミット）
6. `wc -l` で元ファイルのサイズを確認し、まだ500行超なら次のモジュール抽出に進む

## Zigモジュール分割パターン

Databaseのメソッドを別ファイルに分割する場合、Zigではstructにメソッドを外部から追加できないため、以下のパターンを使う:

### パターン: 関数をDatabaseポインタを受け取るfree functionにする

```zig
// eval.zig
const Database = @import("root.zig").Database;
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

## 重要な注意事項

- 一度に1モジュールだけ抽出する。大きな変更を一気にやらない
- 各抽出後に必ず全テスト（unit + differential）を実行する
- テストが壊れたら即座に修正してからコミットする
- Database structのフィールドやpubメソッドは root.zig に残す
- 分割後、抽出されたファイルの行数も500行以内であることを確認する
