# Retired infrastructure (history)

Moved out of the README on 2026-11-15, unchanged.  The current storage tiers are in the README's [Persistence Tiers](../README.md#persistence-tiers) section.

#### Historical: GCP BigQuery Data Platform (2026-04-12 〜 2026-05-11、 retired)

GCP プロジェクト `data-platform-490901` の `geohazard` データセットへ feature matrix + メタデータ + 生データ全 31 テーブルを集約していました (`load_raw_to_bq.py` / `load_to_bq.py`)。 2026-05-11 に **PR #156** で upload step を全削除して retire:

- **理由**: Sandbox 10 GB 上限が `ioc_sea_level` 成長で 98 % 到達。 codebase audit (`grep -rln 'SELECT.*FROM.*geohazard\.'`) で BQ READ パスゼロを確認、 write-only archive 化していた。
- **貢献の記録**: Phase 15h の空間データソース座標ミスマッチバグ (`AVG(so2_column_anomaly) = 0.0`) を BQ 集計クエリで即座に発見、 Phase 15i で 7 ソース一括修正。
- **データ**: 2026-05-12 に dataset 全削除 (34 table + 2 view、 9.3 GB)。 SSD primary tier が PR #157 以降 4 連続 cron success で完動確認 + READ パスゼロにつき archive 不要と判断。 Sandbox quota 93% → 0% 解放。
- **後継**: 直後は RPi5 USB SSD primary tier (PR #157、 2026-05-11) へ移行。 その SSD も 2026-05-27 に物理故障し、 PR #166 で **Hugging Face dataset を新 Canonical tier** に再移行 (上記 Persistence Tiers 参照)。 ad-hoc 集計は HF から `hf download yasumorishima/japan-geohazard geohazard.db --repo-type dataset` で取得 → `sqlite3 ./geohazard.db` で直接クエリ。
