# 開発アイデア

## SQLサポート拡張
- GENERATED COLUMNS

## ストレージ
- B-Tree ベースのページ管理 ← Phase 6d
- SQLite3互換のファイルフォーマット ← Phase 6e
- WAL (Write-Ahead Logging) ← Phase 6f

## 品質保証
- プロパティベーステスト（ランダムSQL生成 + Differential Testing）
- ファジング基盤の構築
- Automated Reasoning: メモリ安全性の形式検証（Zig comptime + 外部ツール）
- ベンチマーク: SQLite3との性能比較

## 開発体験
- エラーメッセージの改善
- EXPLAIN 相当の機能

## 将来構想
- VDBE (Virtual Database Engine) の実装
- SQLite3のテストスイート互換
- C API互換レイヤー
