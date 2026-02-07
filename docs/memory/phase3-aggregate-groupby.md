# Phase 3 集約関数・GROUP BY 実装ノート

## SelectExpr union型の導入

- `Select.columns: []const []const u8` に加え `Select.select_exprs: []const SelectExpr` を追加
- `SelectExpr = union(enum) { column, aggregate }` で通常カラムと集約関数を統一的に扱う
- `columns`は後方互換のために残し、coreの非集約SELECTパスで使用

## 集約関数の設計

- COUNT(*) は行数カウント、COUNT(col) は非NULL値カウント
- SUM/MIN/MAX は整数値のみ対応（現状text型カラムのSUMは0）
- AVG はf64で計算し、`{d}`フォーマットで文字列として返す（SQLite3互換）
- AVG結果の文字列に小数点がない場合は`.0`を付加する

## メモリ管理

- AVGの結果はtext Valueとして新たにallocされる
- `projected_texts`フィールドで集約結果のallocされたtext値を追跡
- `freeProjected`で`projected_texts`を先にfreeし、次に`projected_values`、`projected_rows`の順
- `collectAggTexts`は`select_exprs`を参照し、aggregate式に対応するtext値のみを追跡する（テーブルのtext値との二重free防止）

## GROUP BY

- ハッシュではなく線形探索でグループを構築（小データ向け）
- group_keysとgroup_rowsの2つのArrayListでグループ管理
- 結果のグループはgroup-byカラムの値でソートして出力（SQLite3互換）
- ソートにはproj_rowsとproj_valuesの両方を同じキーでソートする必要がある

## Zig 0.15 固有の注意点

- `std.mem.sort`のコンテキストはstructインスタンスとして渡す
- anonymous structのliteral `.{ .field = val }`はsort contextとして使えるが、分離したstruct型定義がより安全
