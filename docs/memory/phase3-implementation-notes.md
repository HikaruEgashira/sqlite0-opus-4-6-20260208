# Phase 3 実装ノート

## ORDER BY

- `std.mem.sort`はcontextとcomparator関数を分離して渡す
- Zig 0.15では`@This()`でctxの型を参照。無名struct内で関数定義する場合、ctxの型をcomptime解決する必要がある
- ASCがデフォルト。DESC指定時はcomparator内で逆転
- `compareValuesOrder`は`std.math.Order`を返し、ソート用の3値比較を提供

## LIMIT / OFFSET

- SQLite3では`LIMIT -1`は無制限を意味する
- OFFSETはLIMITの後に解析される（`LIMIT n OFFSET m`の順）
- スライスのstart/endをusize範囲にclampしてout-of-bounds防止

## 集約関数

- `SelectExpr`をunion(enum)として導入: `.column`（通常カラム）と`.aggregate`（集約関数呼び出し）
- AVGはf64で計算し、SQLite3互換のフォーマットでテキスト値として返す
- Zigの`{d}`フォーマッタは整数値の場合に小数点を付けないため、手動で`.0`を付加する必要がある
- 集約関数が割り当てたテキスト（AVG結果等）は`projected_texts`で追跡してメモリリーク防止
- `collectAggTexts`で`SelectExpr`がaggregate式のものだけをフィルタリングし、テーブル行のテキスト参照を誤freeしない

## GROUP BY

- グループキーの一致判定に`compareValuesOrder`を再利用
- 出現順でグループを構築し、結果はグループキーでソートしてSQLite3と順序を合わせる
- GROUP BYソート時、proj_rowsとproj_valuesの両方をソートする必要がある（片方だけだとずれる）

## パーサー拡張パターン

- SELECTの解析順: SELECT式 → FROM → WHERE → GROUP BY → ORDER BY → LIMIT → OFFSET → セミコロン
- 各句はoptionalでpeek()でチェックして分岐
- 新しいキーワード追加時はTokenType enum + classifyKeyword + パーサー処理の3箇所を変更

## テスト戦略

- 各機能につき1つのdifferential testファイル
- Differential testのテストランナーは`sed`でプロンプト除去、`grep -v`でバナー行を除外してSQLite3出力と比較
- ユニットテストはパーサーに集中（構造体のフィールド検証）、コアはintegration test中心
