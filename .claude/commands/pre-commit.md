# Pre-commit Check

コミット前の品質チェックを実行してください。

## チェック項目

### 1. ビルド
```bash
zig build
```

### 2. ユニットテスト
```bash
zig build test
```

### 3. 差分テスト
```bash
bash tests/differential/run.sh
```

### 4. ファイルサイズチェック（ADR-0009）
```bash
wc -l packages/*/src/*.zig app/src/*.zig
```

500行超のファイルがある場合、**新たに500行を超えたファイル**（今回の変更で超えた）があれば警告する。

### 5. 変更サマリ
```bash
git diff --stat
```

## 出力形式

全チェックを実行し、結果をまとめて報告:

- Build: OK / FAIL
- Unit Tests: OK / FAIL (N tests)
- Differential Tests: OK / FAIL (N/M passed)
- File Size: OK / WARNING (list violations)
- Changed files: (summary)

全てOKならコミット可能と報告。FAILがあれば修正方法を提案。
