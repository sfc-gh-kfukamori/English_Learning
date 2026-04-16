# リクルート アカウント (ID: 289582) クレジット調査メモ

**調査期間**: 2026年4月  
**対象アカウント**: PRD_RCL_GENESIS (account_id=289582, deployment=gcpuscentral1)  
**クレジット単価**: $1.38/Cr  
**調査ツール**: Snowhouse (`PST.SVCS`, `SNOWHOUSE_IMPORT.gcpuscentral1`)

---

## 1. クレジット消費の概要

### 月次トレンド（主要ウェアハウス）

| 月 | 主な事象 |
|---|---|
| 2025-11 | wh_adobe_datafeed_xsmall が LARGE に拡張（11/13） |
| 2026-01 | wh_adobe_datafeed_xsmall が 331 Cr → 4,206 Cr に急増 |
| 2026-02 | wh_for_work_4xl/X4LARGE系 42台出現でピーク（推定$16,980） |
| 2026-03 | wh_adobe_datafeed_xsmall: 4,536 Cr（月次最大） |
| 2026-04 | 消費が約47%減少（4/4〜） |

### 2026年2月ピークの原因
- 42台の X4LARGE 一時ウェアハウス（wh_for_work_pv_sc*）が 2026-02-19 に作成、2026-03-04 に削除
- wh_for_work_4xl が 1,103 Cr 消費

---

## 2. wh_adobe_datafeed_xsmall の詳細

### 基本情報

| 項目 | 値 |
|---|---|
| 実際のサイズ | LARGE（名前は xsmall のまま） |
| 世代 | GEN1 → **GEN2（2026-04-15 00:09:39 UTC切り替え）** |
| auto_suspend | 60秒 |
| クラスター構成 | シングルクラスター |
| 3月消費 | 4,536 Cr |

### 実行クエリの内訳
全て `ALTER ICEBERG TABLE ... REFRESH` のみ。一般的なDML/SELECTは実行していない。

### GEN1→GEN2 切り替え効果（24時間同一窓比較）

| 指標 | GEN1（4/14） | GEN2（4/15） | 差分 |
|---|---|---|---|
| クエリ数 | 413件 | 434件 | +5% |
| 平均実行時間 | 20.1秒 | 19.6秒 | -2.5%（誤差範囲） |
| P90 | 21.3秒 | 20.9秒 | -2% |
| 合計クレジット | 25.27 Cr | 26.38 Cr | **+4.4%** |
| メータリングスロット | 16 | 16 | 同じ |

**評価**: GEN2への変更は速度・コスト共にほぼ効果なし。

---

## 3. Iceberg REFRESH の仕組み

### 対象テーブル
`"r-data-actionlog"."dwh_adobe_analytics"."sc_raw_datafeed_scXXXX"`

### カタログ構成

| 項目 | 内容 |
|---|---|
| カタログ種別 | CATALOG_SOURCE = OBJECT_STORE |
| ストレージ | GCS `gcs://r-data-actionlog-datafeed-iceberg/` (us-central1) |
| 書き込み元 | **BigQuery（BigLake Metastore利用）** |
| Pub/Sub通知 | 未設定（gcpPubSubSubscriptionName = null） |
| カタログ統合名 | CATALOG_GCS_GENESIS |

### REFRESH処理フロー

```
Step1: metadata.json を GCS から取得・解析（GS処理）
         → スナップショット履歴が大量に含まれる
         → スナップショット蓄積で肥大化 → GS時間増加

Step2: マニフェストリストファイル（.avro）を取得

Step3: マニフェストファイルを全件読み込み（XP処理 = メイン）
         → データファイルの一覧・統計情報が記述されている
         → テーブルのデータファイル数に比例して処理量増加

Step4: Snowflake内部カタログを更新
```

**重要**: REFRESHはParquetデータファイルを一切読まない（メタデータ操作のみ）。ただし、マニフェストファイル（データファイルを記述するメタデータ）はフルスキャンする。

### 処理時間の内訳（重いクエリ vs 軽いクエリ）

| フェーズ | 軽い（<10秒） | 重い（>2分） | 倍率 |
|---|---|---|---|
| GS実行 | 0.2秒 | 41.5秒 | 207倍 |
| XP実行 | 0.7秒 | 243.8秒 | **348倍** |
| ファイルリスト | 0秒 | 0秒 | 変わらず |

---

## 4. 重いREFRESHクエリの分析

### 1日24時間の中の時間分布（GEN1 4/14 24時間）

| 所要時間帯 | 件数 | 消費時間割合 |
|---|---|---|
| 〜10秒 | 337件（82%） | 14% |
| 10〜60秒 | 48件 | 11% |
| 1〜2分 | 9件 | 9% |
| **2〜5分** | **13件** | **32%** |
| **5〜10分** | **5件** | **26%** |
| 10分超 | 1件 | 7% |

→ **件数の5%が時間の65%を消費**

### 重い19件のテーブル（2分超、4/14バッチ）

| ランク | テーブル | 平均所要時間 | 日次頻度 |
|---|---|---|---|
| 1 | sc_raw_datafeed_sc0139 | 13.0分 | 毎日（11/20〜） |
| 2 | sc_raw_datafeed_sc0281 | 12.0分 | 毎日（04/03〜） |
| 3 | sc_raw_datafeed_sc0165 | 11.1分 | 毎日（11/20〜） |
| 4 | sc_raw_datafeed_sc0390 | 9.2分 | 毎日（04/03〜） |
| 5 | sc_raw_datafeed_sc0123 | 8.0分 | 毎日（04/03〜） |
| 6 | sc_raw_datafeed_sc0138 | 6.4分 | 毎日（11/20〜） |
| 7 | sc_raw_datafeed_sc0132 | 5.9分 | 毎日（11/20〜） |
| 8 | sc_raw_datafeed_sc0001 | 5.8分 | 毎日（11/20〜） |
| 9 | sc_raw_datafeed_sc0147 | 5.6分 | 毎日（11/20〜） |
| 10 | sc_raw_datafeed_sc0310 | 5.5分 | 毎日（11/20〜） |
| 11 | sc_raw_datafeed_sc0002 | 5.4分 | 毎日（11/20〜） |
| 12 | sc_raw_datafeed_sc0206 | 5.3分 | 毎日（11/20〜） |
| 13 | sc_raw_datafeed_sc0173 | 3.5分 | 毎日（11/20〜） |
| 14 | sc_raw_datafeed_sc0009 | 4.1分 | 毎日（04/03〜） |
| 15 | sc_raw_datafeed_sc0059 | 2.9分 | 毎日（11/20〜） |
| 16 | sc_raw_datafeed_sc0337 | 3.0分 | 毎日（04/03〜） |
| 17 | sc_raw_datafeed_sc0151 | 2.8分 | 毎日（11/20〜） |
| 18 | sc_raw_datafeed_sc0014 | 2.7分 | 毎日（11/20〜） |
| 19 | sc_raw_datafeed_sc0146 | 2.9分 | 毎日（11/20〜） |

詳細SQLは別ファイル: `recruit_289582_heavy_refresh_queries.sql`

---

## 5. パフォーマンス劣化の根本原因分析

### sc0139 の劣化曲線

| 時期 | 平均実行時間 | XP時間 |
|---|---|---|
| 2025-11-20（作成直後） | **4秒** | 2秒 |
| 2025-12-06 | 8秒 | 7秒 |
| 2026-01-01 | 14秒 | 12秒 |
| 2026-01-05 21:25 | **426秒** | 423秒 ← 転換点 |
| 2026-02-01 | 425秒 | 413秒 |
| 2026-03-17 | 783秒 | 749秒 |
| 2026-04-14 | 609秒 | 529秒 |

**作成直後比: 152倍遅くなっている**

### 1月5日の転換点

2026-01-05 の時系列詳細：

```
01:27 →  15秒（通常）
09:47 →  35秒（最初の兆候）
12:48 → 102秒（スパイク）
13:58 →  16秒（一時回復）
21:25 → 426秒 ← 永続的悪化の始まり
22:31 → 511秒
23:26 → 535秒
```

→ BigQueryが2026-01-05 21:00 UTC頃に大規模データを一括投入したと推定。以降、全REFRESHが恒常的に遅い状態に。

### 2つの悪化要因

| 要因 | 影響フェーズ | 現在の寄与 |
|---|---|---|
| テーブルデータ増加（日次追加） | XP（87%） | **支配的** |
| スナップショット蓄積 | GS（13%） | 加速中 |

REFRESHはマニフェストファイルを**フルスキャン**する（差分処理ではない）ため、テーブルが大きくなるほど毎回の処理量が増加する。

### 全テーブル共通の劣化パターン

Nov 2025時点: 全テーブル 2〜6秒 → 2026-01-15: 全テーブル 76〜791秒 へ急増。
1月のデータ大量投入が全テーブルに影響。

---

## 6. 改善例：sc0151・sc0173

| テーブル | Jan/15 | Feb/15 | Mar/15 | Apr/14 |
|---|---|---|---|---|
| sc0151 | 561秒 | 541秒 | **115秒** | 139秒 |
| sc0173 | 791秒 | 761秒 | **135秒** | 212秒 |

これら2テーブルは2026年2〜3月に大幅改善。BigQuery側で何らかのメンテナンス（コンパクション・スナップショット整理等）が実行された可能性が高い。**他テーブルへの対処法確認のため、BigQueryチームへの問い合わせを推奨。**

---

## 7. スナップショット管理に関する重要な事実

### Snowflakeの仕様（公式ドキュメント確認済み）

> "For tables that use an external catalog, Snowflake does not delete the Iceberg metadata or snapshots from your external cloud storage."

→ **SnowflakeはOBJECT_STOREカタログのGCSファイルを一切削除しない。**

### スナップショット管理の責任者

| 操作 | 担当 |
|---|---|
| スナップショット期限切れ | **BigQuery/BigLake Metastore側** |
| データ投入 | BigQuery側 |
| REFRESH実行 | Snowflake（現状：手動） |

### BigQueryのIcebergスナップショット管理

- BigQueryは「Automatic storage optimization（garbage collection）」を提供するが、これはデータファイルの孤立ファイル削除であり、Icebergスナップショット履歴の削減ではない
- 明示的なスナップショット期限切れコマンドはBigQueryドキュメントで確認できず。BigQueryチームへの確認が必要。
- Snowflake側で実行できる `EXECUTE EXPIRE SNAPSHOTS` コマンドは存在しない（誤情報・訂正済み）

---

## 8. 将来的な改善策

### 短期（今すぐ可能）

| 施策 | 効果 | 担当 |
|---|---|---|
| ウェアハウスをMEDIUMに縮小（GEN2に戻してからLARGEのまま継続中） | 約$3,130/月節約見込み | Snowflake管理者 |
| wh_adobe_datafeed_xsmall の GEN2→GEN1 戻し検討 | 約4.4%コスト削減 | Snowflake管理者 |

### 中期（BigQueryチームとの協力が必要）

| 施策 | 効果 | 担当 |
|---|---|---|
| sc0151・sc0173で実施したメンテナンス内容の確認・横展開 | 重い19テーブルのREFRESH時間を大幅削減 | BigQueryチーム |
| スナップショット管理の定期実行化 | 将来的な劣化防止 | BigQueryチーム |
| 大規模データ投入時の事前通知フロー整備 | 突然の性能劣化防止 | BigQuery + Snowflakeチーム |

### 長期（将来の選択肢）

| 施策 | 効果 | 備考 |
|---|---|---|
| BigLake Metastore REST Catalog統合（Snowflake対応待ち） | 手動REFRESH廃止 | BigLake Metastoreがすでに稼働中のため実現可能性高い |
| GCS Pub/Sub AUTO_REFRESH | REFRESH自動化 | 書き込み頻度が高いため逆効果の可能性あり・要検討 |

---

## 9. 調査で確認・訂正した事項

| 事項 | 当初 | 訂正後 |
|---|---|---|
| GEN2コスト | 「GEN1と同じ」→「38%高い」と修正 | 実測: +4.4%（平均稼働時）。ピーク時は+38%だが平均では限定的 |
| REFRESH ボトルネック | 「GCSファイルリストI/O」 | `DUR_LIST_EXTERNAL_FILES=0秒`。正しくはXP（マニフェスト処理） |
| REFRESH スナップショット処理 | 差分処理と想定 | **フルスキャン**（1/5の時系列データで確認） |
| `EXECUTE EXPIRE SNAPSHOTS` | Snowflakeコマンドとして提示 | **存在しないコマンド**（訂正） |
| `BQ.ICEBERG_EXPIRE_SNAPSHOTS` | BigQueryコマンドとして提示 | **存在しないコマンド**（訂正） |
| REFRESH頻度「4時間おき」 | 算術平均から算出 | 実測: sc_raw_datafeed_sc0367 は**1日1回** |

---

## 10. 主要クエリ・ファイル

| ファイル | 内容 |
|---|---|
| `recruit_289582_credit_investigation_v3.pdf` | クレジット調査レポート（PDF） |
| `recruit_289582_heavy_refresh_queries.sql` | 重い19件のREFRESHクエリ一覧 |

### Snowhouse 接続

```sql
-- セッション開始時に必ず実行
CALL PST.SVCS.SP_SET_ACCOUNT_CONTEXT(289582, 'gcpuscentral1');

-- 主要ビュー
-- SNOWHOUSE_IMPORT.gcpuscentral1.JOB_ETL_V          -- クエリ履歴（SQLはdescriptionカラム）
-- SNOWHOUSE_IMPORT.gcpuscentral1.WAREHOUSE_ETL_V     -- WH設定（SIZEカラム、GENERATION、management_policy はVARCHAR→PARSE_JSON必要）
-- PST.SVCS.WAREHOUSE_METERING_HISTORY               -- クレジット消費（credits_used_computeカラム）
-- SNOWHOUSE_IMPORT.gcpuscentral1.INTEGRATION_ETL_V  -- インテグレーション情報
-- SNOWHOUSE_IMPORT.gcpuscentral1.ICEBERG_EXTERNAL_METADATA_FILE_CHANGELOG_ETL_V -- スナップショット変更履歴
```
