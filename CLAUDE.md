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

## Current Phase

Check `docs/current_tasks/tasks.md` for active work. Development follows phased approach documented in ADRs.
