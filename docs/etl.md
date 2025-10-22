## 処理フロー

### 1. FTP からファイルを取得する

FTP の接続情報は.env ファイルに記載されています。

パスは以下の通りです (拡張子は .xlsx):

- scan_report_daily: /POSReportDaily/report_Xebio_YYYY-MM-DD-YYYY-MM-DD.xlsx
- scan_report_weekly: /POSReport/report_Xebio_YYYY-MM-DD-YYYY-MM-DD.xlsx

daily の場合は、YYYY-MM-DD の部分を両方 JST で 1 日前の日付にしてください
weekylyの場合は、YYYY-MM-DD の部分を、２つ目は直近の日曜日にしてください、1つ目はその日曜日より前の月曜日にしてください

該当ファイルがない場合は、エラーを返す

取得したファイルはリポジトリ直下の `tmp/` に保存され、同名ファイルが存在する場合は上書きされます。
ダウンロードは最大 3 回リトライし (上限は `.env` の `FTP_MAX_RETRIES` で調整可能)、リトライ間隔および各ダウンロード間の待機時間は `FTP_RETRY_DELAY_SECONDS` (既定 5 秒) と `FTP_WAIT_SECONDS` (既定 5 秒) で制御できます。

### 2. 不正値除去

transfer.py の処理で不正値を除去する
ローカル開発の場合のみ、 output/ に結果を保存する。

#### 不正値除去の仕様

- scanner_id​ は null 許可

- Store ID不正値→先頭の(や0を削除
⇒（は削除　0は「区切り位置指定のウィザード」で消える。

- Store IDが「３1029」、「３1037」のように「全角数字＋スペース＋ 4桁の数字」のようになっているパターン→要確認(かんでん)
⇒スキャナIDが空白の場合は削除。　スキャナIDがあるものはstorelistで確認しストアIDを手入力する。

- Store IDない、スキャナIDがない場合→削除
⇒ストアIDが空白のものはないが不正値の場合がある。

- Store IDあり、スキャナIDが無い場合
⇒当該Storeの実績としてカウントする


### 3. s3へのアップロード

s3の接続情報は.env ファイルに記載されています。

不正値除去済みのデータを CSV 形式に変換して s3 にアップロードする。
アップロードするオブジェクトキーは必ず `.csv` 拡張子にしてください (ETL の実装で `.csv` 以外はエラーになります)。

- scan_report_daily: s3://redshift-dwh-prod-uploads/feetaxis/scan_report_daily_YYYYMMDD.csv
- scan_report_weekly: s3://redshift-dwh-prod-uploads/feetaxis/scan_report_weekly_YYYYMMDD.csv

YYYYMMDD の部分は、元ファイル名の YYYY-MM-DD-YYYY-MM-DD の部分の 2 つ目の YYYY-MM-DD をハイフン無しに変換したものにしてください。
