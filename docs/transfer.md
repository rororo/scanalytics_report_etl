## テーブル定義

### scan_report_daily

| No. | 項目論理名                         | 項目物理名            | データ型    | NOT NULL | PK  |
| --- | ---------------------------------- | --------------------- | ----------- | -------- | --- |
| 1   | 測定日                             | scan_date             | date        | 〇       |     |
| 2   | ポイント口座番号                   | point_card_id         | varchar(13) |          |     |
| 3   | 店舗 ID                            | store_id              | varchar(6)  | 〇       |     |
| 4   | 従業員 ID                          | employee_id           | varchar(7)  | 〇       |     |
| 5   | シューズ販売済み                   | shoe_sold             | int         |          |     |
| 6   | マスター DB 該当シューズあり       | shoe_exist_in_db      | int         |          |     |
| 7   | RWA 販売マーク済み                 | shoes_marked_sold_rwa | int         |          |     |
| 8   | インソール販売済み                 | insole_sold           | int         |          |     |
| 9   | 機能性シューズ（フィッティング可） | shoe_functional       | int         |          |     |
| 10  | 推奨サイズ可                       | size_recommendation   | int         |          |     |
| 11  | SafeSize システム固有コード        | safesize_code         | varchar(50) | 〇       |     |
| 12  | 計測端末 ID                        | scanner_id            | varchar(50) |          |     |

### scan_report_weekly

| No. | 項目論理名                         | 項目物理名            | データ型    | NOT NULL | PK  |
| --- | ---------------------------------- | --------------------- | ----------- | -------- | --- |
| 1   | 測定日                             | scan_date             | date        | 〇       |     |
| 2   | ポイント口座番号                   | point_card_id         | varchar(13) |          |     |
| 3   | 店舗 ID                            | store_id              | varchar(6)  | 〇       |     |
| 4   | 従業員 ID                          | employee_id           | varchar(7)  | 〇       |     |
| 5   | シューズ販売済み                   | shoe_sold             | int         |          |     |
| 6   | マスター DB 該当シューズあり       | shoe_exist_in_db      | int         |          |     |
| 7   | RWA 販売マーク済み                 | shoes_marked_sold_rwa | int         |          |     |
| 8   | インソール販売済み                 | insole_sold           | int         |          |     |
| 9   | 機能性シューズ（フィッティング可） | shoe_functional       | int         |          |     |
| 10  | 推奨サイズ可                       | size_recommendation   | int         |          |     |
| 11  | SafeSize システム固有コード        | safesize_code         | varchar(50) | 〇       |     |
| 12  | 計測端末 ID                        | scanner_id            | varchar(50) |          |     |

## データ不正値除去

1. @sample/ から csv を読み込む
2. 以下を基準にして、不正な行を除去して、@output/ に保存する

- store_id: 半角数字以外は不正値
- employee_id: 半角数字 7 桁以外は不正値
- それ以外の not null 制約を準拠しない行

3. 除去した不正な行を、@output/ に保存する
