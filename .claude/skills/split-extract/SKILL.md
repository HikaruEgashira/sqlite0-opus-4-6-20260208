---
name: split-extract
description: Extract a module from a large file following ADR-0009. Moves functions to a new file, updates imports, and verifies all tests pass. Use when a file exceeds 500 lines and needs splitting.
---

# Module Split Extract

指定されたファイルから1つのモジュールを抽出し、分割を実行する。

引数: 抽出元ファイルパス（例: `packages/core/src/root.zig`）

## 手順

1. 対象ファイルを読み、関数一覧と行範囲を把握する
2. [target structure](references/target-structure.md) に基づき、抽出すべきモジュールを特定する
3. **最も依存が少ないモジュールから順に** 1つだけ抽出する（一度に全部やらない）
4. 抽出の実行:
   a. 新ファイルを作成し、対象関数を移動
   b. 元ファイルで `@import` と関数呼び出しを修正
   c. `zig build` でコンパイル確認
   d. `zig build test` でユニットテスト確認
   e. `bash tests/differential/run.sh` で差分テスト確認
5. 全テストが通ったらコミット（1モジュール = 1コミット）
6. `wc -l` で元ファイルサイズを確認し報告

## Zigモジュール分割パターン

Database structのメソッドを外部ファイルの free function に変換する:

```zig
// convert.zig
const std = @import("std");
const value_mod = @import("value.zig");
const Value = value_mod.Value;

pub fn valueToText(allocator: std.mem.Allocator, val: Value) ![]const u8 {
    // self.allocator → allocator に置き換え
}

pub fn formatFloat(allocator: std.mem.Allocator, f: f64) ![]const u8 {
    // ...
}
```

```zig
// root.zig
const convert = @import("convert.zig");

// 呼び出し側: self.valueToText(val) → convert.valueToText(self.allocator, val)
```

## 重要な注意事項

- **一度に1モジュールだけ**抽出する
- 各抽出後に**必ず全テスト**を実行する
- テストが壊れたら即座に修正する
- Database structのフィールドやpub fnは root.zig に残す
- 抽出先ファイルも500行以内であること
- コミットメッセージ: `refactor: extract <module> from <source>`
