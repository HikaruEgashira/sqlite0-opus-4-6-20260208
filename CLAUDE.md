# sqlite0 - Project Intelligence

## Common Commands

```bash
# Build
zig build

# Run REPL
zig build run

# Unit tests (23 tests)
zig build test

# Differential tests against SQLite3 (32 cases)
bash tests/differential/run.sh

# Both tests (CI equivalent)
zig build test && bash tests/differential/run.sh
```

## Project Structure

```
app/src/main.zig          # REPL executable
packages/tokenizer/src/   # Lexical analysis (TokenType, Tokenizer)
packages/parser/src/      # SQL parsing (Statement, Expr, Parser)
packages/core/src/        # Database engine (Database, Table, Value)
tests/differential/       # run.sh + cases/*.sql (SQLite3 comparison)
docs/adr/                 # Architecture Decision Records
docs/memory/              # Implementation notes
docs/current_tasks/       # Active task tracking
docs/ideas/               # Feature backlog
```

Module dependency: `app -> core -> parser -> tokenizer`

## Commit Conventions

```
<type>: <short description in English or Japanese>
```

Types: `feat`, `fix`, `improve`, `refactor`, `docs`, `chore`

## Current Phase

Check `docs/current_tasks/tasks.md` for active work. Development follows phased approach documented in ADRs.
