# 現在のタスク

## Phase 1: 最小パイプライン (進行中)
- [x] プロジェクト構造の構築（build.zig, パッケージ分割）
- [x] Tokenizer実装（キーワード、リテラル、シンボル）
- [x] Parser実装（CREATE TABLE, INSERT, SELECT）
- [x] Core Engine実装（インメモリテーブル、行操作）
- [x] REPL (app/src/main.zig)
- [ ] Differential Testing基盤の構築
- [ ] CI (GitHub Actions) の設定

## 次のタスク
- WHERE句の実装
- DELETE / UPDATE文の実装
- B-Treeストレージの設計
