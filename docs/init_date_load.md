# 初回日付ロード処理 実装計画

## 目的
- `scan_report_daily` および `scan_report_weekly` プレフィックス配下にある既存 CSV を全件取得し、最新日付のキーに集約した単一 CSV を再アップロードする。
- 従来の ETL が期待するファイル命名規則（`scan_report_(daily|weekly)_YYYYMMDD.csv`）を維持しつつ、履歴データを一回で取り込める状態にする。

## 前提・想定
- S3 バケット／プレフィックス情報と認証情報は既存の `.env` と `create_s3_client()`（etl.py）を再利用する。
- daily / weekly ともファイル拡張子は `.csv` であり、同一スキーマを持つ前提で `pandas.concat` による結合が可能。
- オブジェクトキー末尾の `YYYYMMDD` が論理日付を表す（例: `feetaxis/scan_report_daily_20240131.csv`）。
- 既存キーが 1 件も見つからない場合は例外として扱い、処理全体を失敗させる。
- データ件数はメモリに収まる想定。必要に応じて将来的にストリーム結合に差し替え可能な構造にしておく。

## 実装ステップ
1. **設定の組み立て**
   - daily / weekly ごとに対象プレフィックスとアップロード先キーの命名規則をまとめた `DatasetConfig` のようなデータクラスを用意。
   - `create_s3_client()` を利用して S3 クライアント、バケット、プレフィックスを取得。
2. **オブジェクト列挙**
   - `list_objects_v2` をページネーション付きで呼び出し、対象プレフィックス配下のキー一覧を収集。
   - `.csv` のみを対象にし、キー末尾から日付文字列を抽出。パースできないキーはスキップし警告ログを出す。
3. **最新日付の決定**
   - 抽出した日付の最大値を求め、後続アップロード先キー（`scan_report_{granularity}_<latest>.csv`）に反映。
4. **データダウンロード & 結合**
   - 収集したキーすべてについて `get_object` → `pandas.read_csv` で DataFrame 化。
   - 各ファイルから抽出した論理日付を ISO 形式文字列で `scan_date` 列に補完し、既存値がある場合は上書きしない。
   - 列順を揃えるために最初の DataFrame の列リストを基準に `reindex`。
   - 結合後の CSV は `scan_date, point_card_id, store_id, employee_id, shoe_sold, shoe_exist_in_db, shoes_marked_sold_rwa, insole_sold, shoe_functional, size_recommendation, safesize_code, scanner_id, created_at` の順で出力する。
   - `ignore_index=True` で `concat` し、重複行が存在する場合もそのまま保持する（ETL 側の期待に合わせて追加加工は行わない）。
5. **アップロード**
   - 結合後 DataFrame を `to_csv(index=False)` で文字列化し、`put_object` で最新日付キーへアップロード。
   - 既存キーとバケット・プレフィックスの結合には etl.py と同様の `prefix.rstrip('/')` ロジックを共有。
6. **ロギング & ドライラン**
   - 処理の進行状況（対象キー数、スキップ件数、最終行数、アップロード先など）を標準出力に記録。
   - `--dry-run` オプションを実装し、アップロードせずにローカルの `output/` 以下へ書き出す挙動も選択可能にする。

## 検証観点
- daily / weekly 双方で対象キーが見つからない場合のエラー動作。
- 異なる列構成の CSV が混在した際の警告と列そろえ処理。
- `--dry-run` 時に S3 への書き込みが行われないこと、および生成ファイル名が意図通りであること。
- 実データ件数が多い場合のメモリ消費（必要に応じて分割処理の設計余地がある旨を記録）。
- 例外発生時にスタックトレース付きで終了し、どの dataset で失敗したかがログに残ること。
