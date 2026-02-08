---
name: pre-commit
description: Run all pre-commit checks including build, unit tests, differential tests, and ADR-0009 file size validation. Use before committing changes to ensure quality.
---

# Pre-commit Check

コミット前の品質チェックを実行する。

## チェック項目

### 1. Build
```bash
zig build
```

### 2. Unit Tests
```bash
zig build test
```

### 3. Differential Tests
```bash
bash tests/differential/run.sh
```

### 4. File Size Check (ADR-0009)
```bash
wc -l packages/*/src/*.zig app/src/*.zig
```
500行超のファイルがあり、**今回の変更で新たに超えた場合**は警告する。

### 5. Changed Files
```bash
git diff --stat
```

## 出力形式

```
## Pre-commit Report

- Build: OK / FAIL
- Unit Tests: OK / FAIL
- Differential Tests: OK / FAIL (N/M passed)
- File Size: OK / WARNING (list files >500 lines)
- Changed: N files

Verdict: READY TO COMMIT / NEEDS FIX
```

全てOKなら `READY TO COMMIT` と報告。FAILがあれば修正方法を提案。
