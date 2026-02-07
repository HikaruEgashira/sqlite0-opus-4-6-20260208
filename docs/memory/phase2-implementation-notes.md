# Phase 2 実装ノート

## WHERE句の設計

- 単一条件の比較演算（=, !=, <, <=, >, >=）をサポート
- AND/OR/括弧はPhase 3以降
- WHERE句のマッチングはTable構造体のメソッドとして実装し、SELECT/DELETE/UPDATEで共有
- 比較ロジック: 整数同士、テキスト同士の比較をサポート。型不一致は `!=` のみtrue

## トークナイザーの複数文字演算子

- `!=`, `<=`, `>=` は先読み（pos+1）で判定し、単一文字演算子より先にチェック
- この順序が重要: `<=`を先にチェックしないと `<` として消費される

## DELETE文のメモリ管理

- 行削除時は `orderedRemove` を使用（`swapRemove` は順序が変わる）
- 逆順イテレーションで削除（インデックスずれ防止）
- 行のメモリ（テキスト値+values配列）を確実にfree

## UPDATE文の値置換

- 旧テキスト値のfreeが必要（メモリリーク防止）
- 新値はINSERTと同じパース処理（クォート剥がし、整数パース）

## DROP TABLE

- `StringHashMap.fetchRemove` でテーブル名からエントリを取得・削除
- 取得した `Table` の `deinit` を呼んで全リソースを解放

## Zig 0.15特有の注意点

- `std.ArrayList` はfieldとして宣言時に `.{}` で初期化可能
- `var` で宣言しても変更しない場合 `const` にしないとコンパイルエラー
- `orderedRemove` はArrayListのメソッドとして存在（indexを受ける）
- `clearRetainingCapacity` は全要素削除だがメモリは保持
