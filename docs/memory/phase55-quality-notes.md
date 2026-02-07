# Phase 5.5: 品質強化ノート

## 目的
Phase 6 の B-Tree 実装に向けて、現在の SQL 実行エンジンの堅牢性を検証し、バグを修正する。

## 実施内容

### 1. エッジケーステスト追加（テスト駆動開発）

Differential Testing により、SQLite3 との動作を比較しながら5つの新しいテストケースを追加：

#### 25_empty_table.sql
**目的:** 空テーブルに対する集約関数の動作

```sql
CREATE TABLE empty_t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);
SELECT COUNT(*) FROM empty_t;  -- 0
SELECT SUM(value) FROM empty_t;  -- NULL（バグ検出）
SELECT AVG(value) FROM empty_t;  -- NULL（バグ検出）
SELECT MIN(value) FROM empty_t;  -- NULL
SELECT MAX(value) FROM empty_t;  -- NULL
```

**発見:** SUM と AVG が空テーブルで 0 を返していた（SQLite3 は NULL）

#### 26_order_by_null.sql
**目的:** ORDER BY での NULL 値の扱い

```sql
CREATE TABLE items (id INTEGER, name TEXT, price INTEGER);
INSERT INTO items VALUES (1, 'apple', 100);
INSERT INTO items VALUES (2, NULL, 200);
INSERT INTO items VALUES (3, 'cherry', NULL);
INSERT INTO items VALUES (4, NULL, NULL);
SELECT * FROM items ORDER BY name ASC;   -- NULL が先
SELECT * FROM items ORDER BY price DESC; -- NULL が後ろ（DESC は未対応か確認）
```

**結果:** ASC/DESC での NULL ソート順は SQLite3 と一致

#### 27_group_by_null.sql
**目的:** GROUP BY でのNULL 値のグループ化

```sql
CREATE TABLE logs (id INTEGER, category TEXT, value INTEGER);
INSERT INTO logs VALUES (1, 'a', 10);
INSERT INTO logs VALUES (2, NULL, 30);
INSERT INTO logs VALUES (3, NULL, 50);
SELECT category, COUNT(*) FROM logs GROUP BY category;  -- NULL を1グループとして扱う
```

**結果:** NULL 値のグループ化は正しく動作

#### 28_sequential_ops.sql
**目的:** 連続的な DML 操作（INSERT → UPDATE → DELETE → SELECT）

```sql
CREATE TABLE data (id INTEGER, val INTEGER);
INSERT INTO data VALUES (1, 100), (2, 200), ...;
UPDATE data SET val = 999 WHERE id = 3;  -- 中間の行を更新
DELETE FROM data WHERE id = 2;           -- 別の行を削除
SELECT * FROM data;                      -- 結果の正確性確認
SELECT COUNT(*), SUM(val) FROM data;     -- 集約が正しい値を返すか
```

**結果:** 連続操作後のメモリ状態と集約結果は正確

#### 30_null_compare.sql
**目的:** NULL 比較演算子のセマンティクス

```sql
CREATE TABLE t (id INTEGER, a INTEGER, b TEXT);
INSERT INTO t VALUES (1, 10, 'hello');
INSERT INTO t VALUES (2, NULL, 'world');
INSERT INTO t VALUES (3, 30, NULL);

SELECT * FROM t WHERE a = NULL;       -- 0行（SQL 標準）
SELECT * FROM t WHERE a IS NULL;      -- 2行と3行（正しい NULL チェック）
SELECT COUNT(*) FROM t WHERE a IS NULL;  -- 2
```

**結果:** NULL 比較のセマンティクスは正しく実装されている

### 2. バグ修正

#### Bug #1: SUM on empty table
**原因:** packages/core/src/root.zig の computeAgg() 関数

```zig
// 修正前
.sum => {
    var total: i64 = 0;
    for (rows) |row| {
        if (row.values[col_idx] == .integer) {
            total += row.values[col_idx].integer;
        }
    }
    return .{ .integer = total };  // 行がなくても 0 を返す
},

// 修正後
.sum => {
    var total: i64 = 0;
    var has_value = false;
    for (rows) |row| {
        if (row.values[col_idx] == .integer) {
            total += row.values[col_idx].integer;
            has_value = true;
        }
    }
    if (!has_value) return .null_val;  // 非NULL値がなければ NULL
    return .{ .integer = total };
},
```

#### Bug #2: AVG on empty table
**原因:** 同じく computeAgg() の AVG ケース

```zig
// 修正前
if (cnt == 0) return .{ .integer = 0 };  // cnt=0 で 0 を返す

// 修正後
if (cnt == 0) return .null_val;  // NULL を返す
```

#### Bug #3: Test runner NULL output filtering
**原因:** tests/differential/run.sh の sed 処理が不完全

```bash
# 修正前
sqlite0_output=$("$SQLITE0" < "$case_file" 2>/dev/null | sed 's/^sqlite0> //' | ...)
# sed の s/// は1回のみ置換のため、プロンプトが重複している行を処理できない

# 修正後
sqlite0_output=$("$SQLITE0" < "$case_file" 2>/dev/null | sed -e ':a' -e 's/^sqlite0> //' -e 'ta' | ...)
# ラベル :a と ta で繰り返し置換を実現
```

### 3. テスト基盤の改善

#### テストランナーの堅牢化

**変更点:**
1. **NULL 出力の正確な処理**
   - SQLite3 の NULL 出力（空行）を保持
   - sqlite0 のプロンプト重複を削除
   - 空行フィルタリングを両方に統一

2. **プロンプト処理の修正**
   - Recursive sed で `sqlite0> ` を全て除去
   - 複数の SELECT 文がある場合のプロンプト重複に対応

```bash
# 繰り返し置換パターン（sed）
sed -e ':a' -e 's/^pattern/replacement/' -e 'ta'
```

### 4. テスト統計

| フェーズ | テスト数 | 通過数 | 失敗数 | 備考 |
|---------|---------|--------|--------|------|
| Phase 1-5 | 24 | 24 | 0 | 新規テスト追加前 |
| Phase 5.5 | +5 | 29 | 0 | 全て通過 |
| バグ修正後 | 29 | 29 | 0 | 再確認 |

## Expression Evaluation の判断

Phase 5.5 では Tokenizer のみを拡張し、完全な Expression Evaluation は見送った。理由：

1. **複雑性が高い**
   - Parser での優先度制御の実装が必要
   - 括弧、単項演算子、二項演算子の全て対応
   - Expr AST の深いツリー構造とメモリ管理

2. **現在のサポートが十分**
   - GROUP BY, JOIN, Subquery, Aggregate function で大部分のクエリに対応
   - 算術式がなくても多くの実務的なクエリが可能

3. **B-Tree 優先**
   - ストレージ基盤の確立を先に進める方が戦略的
   - Expression Evaluation は Phase 6 後で段階的に実装可能

## 次のステップ

### Phase 6 への引き継ぎ事項

1. **テスト基盤の確立**
   - 29 個のテストケースで各機能を検証
   - Differential Testing により SQLite3 との互換性を常時確認

2. **品質レベル**
   - SQL 実行エンジンの安定性が確保された
   - NULL 処理、空テーブル、複合操作などエッジケースを網羅

3. **技術的準備**
   - Tokenizer で算術演算子が定義済み
   - Parser で Expr 型の基本構造が定義済み
   - 完全な Expression Evaluation はPhase 6以降で実装可能

### Phase 6 の計画

B-Tree ストレージの設計・実装：
- ページベースのメモリ管理（通常4KB）
- SQLite3 互換のページレイアウト
- Cell array, Freelist, Page header の実装
- 永続化とランダムアクセス

## おわりに

Phase 5.5 は短期でありながら、品質強化の観点から非常に価値が高かった。エッジケーステストにより3つのバグを発見・修正し、テスト基盤を確固たるものにした。Phase 6 への移行に際して、既存の SQL 機能が安定していることを確認できたことは大きな成果である。
