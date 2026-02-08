# Phase 6d-4: INTERSECT/EXCEPT + GLOB

## 実施内容

### INTERSECT / EXCEPT
- トークナイザに`kw_intersect`, `kw_except`追加
- パーサー: `SetOp` enum（union_all, union_distinct, intersect, except）導入
- `UnionSelect.is_all`を`UnionSelect.set_op`に変更
- `parseUnion`を`parseSetOp`にリネーム・拡張
- 実行エンジン: `executeSetOperands`でSELECT結果を個別に収集し、set_opに応じて合成
  - INTERSECT: 全operandに含まれる行のみ（重複排除）
  - EXCEPT: 最初のoperandにのみ含まれる行（重複排除）

### GLOB演算子
- `kw_glob`トークン追加
- `BinOp.glob`追加、`parseComparison`で認識
- `globMatch`関数: `*`（任意文字列）、`?`（単一文字）、case-sensitive
- `evalExpr`でGLOB評価追加

## テスト結果
- ユニットテスト: 23/23 passing
- Differential Test: 49/49 passing（前回45 → 追加4件）

## 注意事項
- differential testでSQL comment（`--`）は使えない（REPLがコメント未対応）
- Zig 0.15.2が必要（ArrayListのunmanaged APIを使用）
