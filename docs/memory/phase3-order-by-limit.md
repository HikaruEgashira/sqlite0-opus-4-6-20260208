# Phase 3 実装ノート: ORDER BY と LIMIT/OFFSET

## ORDER BY の設計

### トークナイザー追加
- `ORDER`, `BY`, `ASC`, `DESC`: 新しいキーワード追加
- これらはパーサーで処理される

### パーサーの変更
- `OrderByClause` struct追加: `column`, `is_desc` (DESC指定時true)
- `Select`構造体に拡張:
  - `order_by: ?OrderByClause` - ORDERBYの有無
  - `limit: ?u64` - LIMITの値
  - `offset: ?u64` - OFFSETの値
- `parseOrderByClause()`: ORDER BY <col> [ASC|DESC]を解析
- デフォルトはASC

### コア実行エンジン
- `RowComparator` struct: std.mem.sort用のcontext
  - `col_idx`, `is_desc`, `allocator`を保持
  - `compare()`メソッドで値の大小比較
- `compareRowsByValue()`: IntegerとTextの順序比較
- SELECT実行時のORDER BY適用:
  1. WHERE条件で行をフィルタリング
  2. ORDER BYが指定されていれば、マッチングした行をソート
  3. LIMIT/OFFSETで範囲制限
  4. 列投影を適用

## LIMIT/OFFSET の設計

### パーサー
- LIMIT直後の整数リテラルをu64で解析
- OFFSETはLIMITの後で指定可能 (LIMIT n OFFSET m)
- 両方optional

### コア実行エンジン
- matching_rows (WHERE適用後) の実行シーケンス:
  1. ORDER BYが指定されていればソート
  2. offset_val = offset orelse 0
  3. final_count = 全行数から offset_val, limit_val を考慮して計算
  4. offset_val から offset_val + final_count までの行を投影
- オフセットが全行数以上の場合は空の結果
- limitが指定されない場合、offsetからEOFまでを返す

## 実装の考慮点

### メモリ管理
- matching_rows (ArrayList) は関数内でdeferでdeinit
- projected_rows/projected_values は既存の freeProjected()で管理

### 型の互換性
- Zig std.mem.sort は context の型を厳密に要求
  - inline struct `.{ ... }` ではなく、先にstruct値を作成してから渡す
  - `const ctx = RowComparator{ ... }; std.mem.sort(..., ctx, ...)`

### SQLite互換性
- ASC/DESC: SQLiteはDESCをサポート、デフォルトASC
- LIMIT/OFFSET: SQLiteと同じセマンティクス
- 複数行ソート: 現在は単一列のみ対応（複合キーソートはPhase 4で検討）

## テストケース

- 10_order_by.sql:
  - INTEGER列、TEXT列でのソート
  - ASC (默认) と DESC

- 11_limit_offset.sql:
  - 単純な LIMIT
  - LIMIT + OFFSET の組み合わせ
  - LIMIT が全行数より大きい場合

## パフォーマンス（現在）

- ORDER BY: O(n log n) の std.mem.sort を使用
- メモリ上のArrayListをソートしているため、データセット全体がメモリに載る必要がある
- B-Tree実装 (Phase 4+) で大規模データセットに対応予定
