# ADR-0008: Phase 6c WHERE句式基化のリスクアセスメント

## ステータス
審議中（リスク分析完了）

## 背景
Phase 6a（Expression Evaluation）と Phase 6b（CASE WHEN、LIKE、複数SET UPDATE）で式評価基盤が整備された。
次ステップとして、WHERE句全体をExpr ASTとして統一する Phase 6c に着手予定。

**現状:**
- WhereClause構造体: `column`, `op`, `value`, `extra[]`, `connectors[]` で表現
- matchesWhere / matchesSingleCondition (Table メソッド)
- matchesWhereWithSubquery (Database メソッド)で相互に処理
- SELECT, DELETE, UPDATE, JOIN で使用
- Expr に logical_and / logical_or がない

**移行計画:**
- Exprに `logical_and`, `logical_or` BinOp 追加
- Select/Delete/Update に `where_expr: ?*const Expr` フィールド追加
- parseWhereExpr 関数を追加（WHERE句をExprツリーとしてパース）
- evalExpr で AND/OR 評価を実装
- executeSelect, executeDelete, executeUpdate, executeJoin で where_expr ベースに切替
- 旧WhereClause系を削除（後方互換性維持後）

## 識別されたリスク

### リスク1: サブクエリの式ベース化（深刻度: 中高）

**現状:**
- `col IN (SELECT ...)` と `col op (SELECT ...)` は WhereCondition.subquery_sql に SQL文字列を保持
- matchesConditionWithSubquery で実行時にパースして評価
- 複数条件の場合、extra[] / connectors[] で AND/OR 処理

**式ベース化での課題:**
1. **Expr型の拡張が必要**
   - `subquery_expr: ?*const Expr` フィールドを持つバリアント
   - または、比較演算の右辺に SELECT式を表現できる仕組み
   - IN演算子を BinOp に追加し、右辺に SELECT式を埋め込む

2. **evalExpr での subquery 評価**
   - サブクエリ実行は executeSubquery を呼び出す（Database が必要）
   - 現在の evalExpr は `Database, Table, Row` を受け取るので対応可能
   - しかし、IN リストの複数値マッチングロジックを evalExpr に組み込む必要

3. **IN演算子の複数値ハンドリング**
   - `WHERE col IN (SELECT id FROM table)` → 複数行のマッチング
   - 現在の BinOp は単値比較が主（eq, ne, lt等）
   - IN演算子は単値と複数値セットの比較
   - CompOp では in_subquery として定義済みだが、BinOp に統合するかは設計判断

**対策:**
- **段階的統合:** まず scalar subquery（1値）を Expr で表現し、IN（複数値）は後続フェーズに延期
- **Expr拡張:** `subquery_select: *const Statement.Select` バリアントを追加（サブクエリAST）
- **evalExprの改造:** Database メソッドに追加し、サブクエリ実行を integrate
- **メモリ管理:** サブクエリ結果の一時値を allocator で管理し、確実に解放

**完了条件:**
- [ ] Exprに subquery バリアント を追加
- [ ] parseWhereExpr で `col = (SELECT ...)` をパース
- [ ] evalExpr でサブクエリ実行、 compareValues で1値マッチング
- [ ] 新規 differential test: subquery_expr.sql 追加（2-3 ケース）

---

### リスク2: IS NULL / IS NOT NULL の式表現（深刻度: 中）

**現状:**
- `col IS NULL` / `col IS NOT NULL` は CompOp.is_null / is_not_null
- parseWhereCondition で kw_is トークンを検出、特別扱い
- matchesSingleCondition で value == "" の場合に NULL チェック

**式ベース化での課題:**
1. **unary operator の未対応**
   - BinOp は二項演算（左右の Expr）
   - `col IS NULL` は単項（右辺なし）
   - parser の parseComparison では binary のみ想定

2. **parseWhereExpr での実装**
   - parseComparison で `IS NULL` をキャッチする層を追加
   - または parseUnary 層を追加し、一項演算を handleする

3. **Expr型設計**
   - `is_null_check: *const Expr` バリアント（col名を保持）
   - または、BinOp に right=null（void）の特殊ケースを許可

**対策:**
- **BinOp null右辺許可:** BinOp の right を `?*const Expr` にし、NULL check用に right=nullを許可
  - 利点: 既存二項演算の枠組みに統一
  - 欠点: 右辺がない場合の evalExpr ロジックを複雑化

- **unary_op バリアント追加:** `unary_op: struct { op: UnaryOp, operand: *const Expr }`
  - UnaryOp enum: is_null, is_not_null（後で not, unary_minus等拡張可能）
  - 利点: 明示的、将来の拡張が容易
  - 欠点: Expr型がさらに複雑化

**推奨:** unary_op バリアント追加（長期的保守性を重視）

**完了条件:**
- [ ] Expr に unary_op バリアント追加（UnaryOp enum新規）
- [ ] parseWhereExpr で `col IS NULL` / `IS NOT NULL` をパース
- [ ] evalExpr で unary_op 評価実装
- [ ] 既存 test 19_null.sql が passing（後方互換維持）

---

### リスク3: JOIN での WHERE処理（深刻度: 中高）

**現状:**
- executeJoin で、左右テーブルのカラムを結合した仮想スキーマを構築
- where.column で column 検索時、左テーブルを優先、なければ右テーブル
- 最後に単一の where.column, where.op, where.value で WHERE フィルタ適用

**式ベース化での課題:**
1. **式中のカラム参照の解決**
   - `LEFT JOIN right ON t1.id = t2.id WHERE t1.salary > 100 AND t2.name = 'bob'`
   - Expr 内の column_ref 「t1.salary」「t2.name」を結合スキーマで解決
   - 現在は column name のみ（テーブル修飾なし）

2. **mixed-table expressions**
   - 結合後の仮想行で評価する場合、column_ref を正しいインデックスにマッピング
   - `t1.col` と `t2.col` が競合（両テーブルに同名カラムがある場合）

3. **LEFT JOIN での NULL伝播**
   - 右テーブル未マッチ時、右側カラムが NULL
   - Expr 評価時に NULL が正しく扱われるか確認

**対策:**
- **テーブル修飾の導入（段階的）**
  - Phase 6c では単一テーブル WHERE のみサポート（JOIN は後続へ延期）
  - または、parseWhereExpr 時に左テーブルを優先的に解決、曖昧性がある場合はエラー

- **joined row context への適応**
  - evalExpr に追加パラメータ `?*const Table` 左テーブル、 `?*const Table` 右テーブル
  - column_ref 解決時、両テーブルを検索し、インデックスをマップ

**推奨:** Phase 6c では単一テーブル WHERE に限定、JOIN での複合式は Phase 6c-ext へ延期

**完了条件:**
- [ ] executeJoin は既存 WhereClause のまま保持（expr 化は後続）
- [ ] SELECT / DELETE / UPDATE WHERE のみを expr 化
- [ ] テスト: 結合テーブルでの旧 WhereClause 動作は維持

---

### リスク4: 後方互換性（テスト維持）（深刻度: 低〜中）

**現状:**
- ユニットテスト: 23/23 passing
- Differential tests: 36/36 passing
- 特に重要: 05_where_clause.sql, 06_where_operators.sql, 16_where_and_or.sql, 19_null.sql, 22_subquery.sql

**式ベース化でのリスク:**
1. **Parser変更によるパース失敗**
   - parseWhereExpr 実装に不具合 → パースエラー → テスト失敗
   - 特に AND/OR の優先度（AND が OR より高い）の処理

2. **evalExpr ロジック不具合**
   - NULL handling の誤差
   - AND/OR の short-circuit 評価（Expr評価時に &&/|| を正しく使う）
   - LIKE, 比較演算の結果が 0/1 に正規化されているか

3. **メモリ管理による undefined behavior**
   - 旧 WhereClause 削除時、解放漏れ
   - expr ツリー deletion で double-free

**対策:**
- **段階的マイグレーション**
  1. 旧 WhereClause を残す（互換モード）
  2. parseWhereExpr で新 Expr 形式をパース
  3. executeSelect 等で where_expr が null でない場合は expr 評価、null なら WhereClause 評価
  4. 全テスト passing 確認後、WhereClause 削除

- **差分テスト増強**
  - 既存 36ケース + 新規 6-8 ケース（expr系, subquery, null, AND/OR）
  - 最小: 42-44/44 passing を目指す

**完了条件:**
- [ ] 全既存 test passing（SELECT where_expr でも old WhereClause でも同結果）
- [ ] 新規 differential test 8-10 件追加実装
- [ ] unit test: parseWhereExpr, evalExpr on WHERE context

---

### リスク5: メモリリーク（Exprツリー管理）（深刻度: 中）

**現状:**
- freeExpr, freeExprSlice が実装済み（Phase 6a）
- SELECT result_exprs は defer Parser.freeExprSlice で自動管理

**式ベース化でのリスク:**
1. **where_expr の解放漏れ**
   - Select.where_expr, Delete.where_expr, Update.where_expr は parser で allocated
   - execute() 内で defer で解放する必要あり
   - サブクエリ含む場合、ネストされた Statement も解放対象

2. **サブクエリ Statement の深さ**
   - `WHERE col IN (SELECT * FROM t WHERE x IN (SELECT ...))`
   - 再帰的に Statement が nested
   - Parser がこれらを allocate → execute で解放 → メモリ leak risk

3. **Statement の複合解放**
   - wheren_expr だけでなく、select_exprs も自由
   - set_values（Update）内の式参照も検討

**対策:**
- **Statement deinit 関数の整備**
  ```zig
  pub fn deinitStatement(allocator: Allocator, stmt: Statement) void {
      switch (stmt) {
          .select_stmt => |sel| {
              allocator.free(sel.columns);
              allocator.free(sel.select_exprs);
              Parser.freeExprSlice(allocator, sel.result_exprs);
              if (sel.where_expr) |expr| Parser.freeExpr(allocator, expr);
              // ... 他フィールド
          },
          ...
      }
  }
  ```

- **allocator arena の使用（検討）**
  - Parser の一時領域を arena allocator 化し、parse後に全 free
  - ただし、parsed statement は database lifetime で保持される場合あり（検討必要）

**完了条件:**
- [ ] Statement union に deinit 関数追加（またはhelper）
- [ ] execute() で defer stmt_deinit() を呼び出し
- [ ] valgrind / LeakSanitizer で leak なし

---

## リスク マトリクス（優先度）

| # | リスク | 深刻度 | 発生確度 | 優先度 | 対策の難易度 |
|----|--------|--------|---------|--------|------------|
| 1 | サブクエリ式ベース化 | 中高 | 高 | **高** | 高 |
| 2 | IS NULL表現 | 中 | 中 | 中高 | 中 |
| 3 | JOIN WHERE処理 | 中高 | 中 | 中 | 高 |
| 4 | 後方互換性 | 中 | 中 | 中 | 中低 |
| 5 | メモリリーク | 中 | 中 | 中 | 低 |

## 推奨アプローチ（段階的リスク軽減）

### Phase 6c-1: 基盤整備（1.5-2週間）
- [ ] Expr に logical_and, logical_or BinOp 追加
- [ ] Expr に unary_op バリアント追加（UnaryOp enum）
- [ ] parseWhereExpr 関数実装（単一テーブル、複合条件AND/OR対応、unary is_null）
- [ ] evalExpr に AND/OR, IS NULL 評価ロジック追加
- [ ] 既存 WHERE test (5, 6, 16, 19) passing確認

**テスト:** 既存 5 件 passing

### Phase 6c-2: SELECT WHERE式化（1週間）
- [ ] Select.where_expr フィールド追加
- [ ] parseSelect で where_expr を populate
- [ ] executeSelect で where_expr ベース評価に切替（後方互換: wheren が null なら old路）
- [ ] DELETE WHERE 同様に実装
- [ ] UPDATE WHERE 同様に実装

**テスト:** 既存 36 件 passing

### Phase 6c-3: サブクエリ統合（2週間、リスク最高）
- [ ] Expr サブクエリバリアント（subquery_select: Statement.Select）を追加
- [ ] parseWhereExpr で `col = (SELECT ...)` / `col IN (SELECT ...)` をパース
- [ ] evalExpr でサブクエリ実行、複数値マッチング実装
- [ ] 既存 22_subquery.sql passing 確認
- [ ] 新規 differential: subquery_expr_*.sql (2-3 件)

**テスト:** 既存 36 + 新 3 = 39 passing

### Phase 6c-4: JOIN WHERE延期
- [ ] executeJoin は phase 6c では expr 化せず（複雑性を避ける）
- [ ] JOIN WHERE テスト (14, 15) は既存 WhereClause のまま動作確認
- [ ] JOIN expr 化は Phase 6c-ext に explicit 記録

**テスト:** JOIN WHERE は既存 WhereClause で継続、test passing

### Phase 6c-5: メモリ管理・クリーンアップ（1週間）
- [ ] Statement deinit helper 実装
- [ ] execute() で defer statement_deinit() 適用
- [ ] valgrind / asan で leak チェック
- [ ] 旧 WhereClause struct を削除（expr置換後）

**テスト:** 43-44/44 (新规4-5件含む) passing

## 撤退ポイント

以下いずれかの場合、Phase 6c を一部延期：

1. **サブクエリ統合で複数行マッチングが実装困難**
   - → scalar subquery のみに限定、IN は 後続へ延期
   - → differential test で subquery_scalar.sql (2件) のみ追加

2. **メモリ leak が予期せず多発**
   - → arena allocator 導入検討
   - → Phase 6c 期間延長申請

3. **既存 test が 3件以上失敗（root cause不明）**
   - → Phase 6c を一時中止、6b のバグ修正に転向

## 実装カテゴリ分類

### カテゴリA: 必須（Phase 6c 完了条件）
- logical_and, logical_or, unary_op 追加
- parseWhereExpr 実装
- evalExpr AND/OR/IS NULL
- executeSelect/Delete/Update where_expr化
- 既存テスト 36 全 passing

### カテゴリB: 強く推奨（品質）
- サブクエリスカラー統合
- Statement deinit / memory 管理
- 新規 differential +4-5

### カテゴリC: 後続フェーズへ延期
- IN（複数値マッチング）サブクエリ
- JOIN WHERE expr 化
- テーブル修飾カラム（t1.col等）
