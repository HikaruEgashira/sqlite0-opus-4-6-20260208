---
name: split-check
description: Inspect all source file sizes and report ADR-0009 module splitting rule violations. Use when checking whether files exceed the 500-line limit or before adding new features.
---

# Module Split Check

プロジェクト全体のファイルサイズを検査し、モジュール分割ルール（ADR-0009）への違反を報告する。

## 手順

1. `wc -l packages/*/src/*.zig app/src/*.zig` を実行してファイル行数を取得
2. 以下の基準で判定:
   - **500行超**: 分割が必要（VIOLATION）
   - **300-500行**: 次の機能追加時に分割を検討（WARNING）
   - **300行以下**: 正常（OK）
3. 違反ファイルに対して、[target structure](references/target-structure.md) に基づいた具体的な分割案を提示
4. 分割案には以下を含めること:
   - 抽出対象の関数名と行範囲
   - 新モジュール名
   - 依存関係（どのモジュールがどのモジュールを参照するか）

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
