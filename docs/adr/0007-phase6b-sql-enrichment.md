# ADR-0007: Phase 6b - SQL機能充実をストレージより優先

**Date:** 2026-02-08
**Status:** Accepted

## Problem

Phase 6a（式評価エンジン）完了後の次フェーズで、当初はPhase 6b = ストレージ抽象化 + B-Treeを予定していた。しかし、この大規模な変更はSQL機能が未完のまま進める場合、以下のリスク/デメリットがある:

1. **検証困難**: ストレージ層変更時、SQL機能の制限がリグレッション検出を難しくする
2. **テスト不足**: Differential Testingの効果が限定的（Storage依存の機能が先行）
3. **段階的価値提供**: SQL機能の追加により、メモリベースのままでも利用価値が向上

## Decision

**Phase 6bをSQL機能充実に再定義し、ストレージ抽象化(現Phase 6d相当)は後順とする。**

### 実装フェーズ化

| フェーズ | 内容 | テスト |
|---------|------|--------|
| **Phase 6b-1** | 比較演算子式統合 + 複数SET UPDATE | Diff Test 34, 35 |
| **Phase 6b-2** | CASE WHEN式 + LIKE演算子 | Diff Test 36, 37 |
| **Phase 6c** | WHERE句式ベース化（オプション） | 2-3件 |
| **Phase 6d** | ストレージ抽象化 + B-Tree | Adapter層テスト |
| **Phase 6e** | ファイルI/O + フォーマット | Diff Test |
| **Phase 6f** | WAL実装 | Durability検証 |

## Rationale

1. **メモリベース≠検証不完**: SQL機能はストレージ層に依存しないため、完全にメモリ内で検証可能
2. **テスト効率**: 各SQL機能の追加時、Differential Testing(SQLite3比較)で即座に正確性確認
3. **後続基盤**: ストレージ移行時に豊富なSQL機能のテストシナリオが利用可能
4. **リスク軽減**: 大規模なストレージ層変更を避け、段階的な機能追加で品質を維持

## Consequences

### Positive
- Differential Test数が逐次増加（検証基盤強化）
- 各フェーズで完結した機能提供（trunk-based development向け）
- ストレージ移行時のテスト負荷が削減

### Negative
- B-Treeへの移行が遅延（5~6ヶ月想定）
- 大規模データでのメモリ制限が継続（Phase 6dまで）

## Acceptance Criteria

1. ✅ Phase 6b-1完了: 比較演算子 + 複数SET UPDATE実装、Diff Test 34-35 pass
2. ✅ Phase 6b-2完了: CASE WHEN式 + LIKE演算子、Diff Test 36-37 pass
3. ✅ 全Diff Test: 36/36 passing（Regression 0）

## Related

- [ADR-0006: Phase 6a - Expression Evaluation](0006-phase6a-expression-evaluation.md)
- [Principal: Differential Testing](../ideas/backlog.md)
