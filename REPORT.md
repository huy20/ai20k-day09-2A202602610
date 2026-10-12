# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phung Quang Minh Huy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602610 |
| GitHub | huy20 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-01037 |

## Kết quả cuối

- `dataset_version` của bạn: `b4fa2af6fbed`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `709 / 159 / 151` (tổng 1019, 18/4/4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Bốn khác biệt giữa bronze A và B: (1) tên cột khác nhau (A: `ts_utc, speed_mps, heading_deg, gas, brake`; B: `timestamp_ms, spd_kmh, hdg, throttle_pct, brake_pct`); (2) đơn vị tốc độ khác (A là m/s, B là km/h); (3) kiểu thời gian khác (A là chuỗi ISO UTC, B là epoch mili-giây số nguyên, và đồng hồ thiết bị B bị lệch so với video); (4) bàn đạp khác thang (A là phân số 0..1, B là phần trăm 0..100). Pipeline giữ **mỗi nguồn một bảng** vì hai nguồn lệch schema (schema drift) – gộp ngay sẽ mất dữ liệu gốc và không lần lại được; để mọi cột ở dạng chữ vì bronze chỉ **ghi nguyên bản, chỉ thêm**, việc ép kiểu/chuẩn hoá dời sang silver nơi hai nguồn thực sự được hoà giải. Nhờ vậy nếu logic silver đổi, ta vẫn dựng lại được từ bronze.

## Bước 2 · Silver telemetry

### Câu 2
Hai nguồn được đưa về một schema bằng các model staging: B được đổi tên cột, đổi km/h sang m/s (`/3.6`), đổi epoch ms sang timestamp giây, và **trừ độ lệch đồng hồ** `telemetry_clock_offset_ms` (1500 ms) khai báo trong `sources.json`/`bronze.video_files`. Bằng chứng: `bat_dau` của drive B = `2025-10-08 14:30:00.009000+00:00`, trùng thời điểm bắt đầu video; tốc độ tối đa hợp lý (71 / 100 km/h) và test `in_range 0..70 m/s` đạt.

| drive | duplicate | speed_missing | speed_out_of_range | sạch | nhận |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3234 |

Mỗi drive 53 dòng bị loại (106 tổng). Dòng lỗi vào `silver_telemetry_rejects` kèm lý do thay vì bị xoá, vì xoá im lặng làm mất khả năng kiểm toán và không thể điều tra lại; đồng thời bất biến `sạch + quarantine == nhận` cho phép kiểm chứng mỗi dòng vào đúng một chỗ.

### Câu 3
`max_gap_ms` là `warn` vì nó phản ánh cảm biến mất tín hiệu – một tình huống đã biết và chấp nhận được, chỉ cần báo chứ không nên chặn pipeline. `in_range 0..70 m/s` là `error` vì tốc độ vô lý nghĩa là dữ liệu sai/hỏng, phải chặn để dữ liệu xấu không đi tiếp. Thí nghiệm `break-gate`: nới `max_speed_mps` từ 70 lên 1000 thì test `in_range_silver_telemetry_speed_mps__70__0` **FAIL 6 dòng** (PASS=5 WARN=1 ERROR=1); khôi phục luật gốc thì ERROR=0. Điều này cho thấy cổng chất lượng thực sự bắt được dữ liệu giả và dừng đúng chỗ.

## Bước 3 · Silver frames

### Câu 4
Theo `idempotency.json` (trip-00003): trước = 1246 dòng, sau hai lần chạy lại = 1246 dòng, dấu vân tay trước/sau đều `6c7480dfde7e` – **không đổi**. Kiểu ghi sai `append` cho một trip 60 khung thì sau hai lần thành 120 dòng – **nhân đôi 2×**. Delta `overwrite` theo partition (vị từ trên `trip_id`) an toàn hơn xoá-rồi-ghi vì nó là một giao dịch ghi nguyên tử chỉ thay các file của partition đó; nếu lỗi giữa chừng, commit log của Delta vẫn trỏ về phiên bản cũ nên bảng không bị nửa vời, trong khi "xoá rồi ghi" có thể mất dữ liệu hoặc để lại trạng thái hỏng.

### Câu 5
Khung **đen** là lỗi camera/cảm biến (độ sáng ~0) nên bị **loại** (`is_black`); khung **tối** là cảnh thiếu sáng thật (đường hầm, bóng râm), vẫn dùng được nên được **gắn cờ nhưng giữ lại** (`is_dark`). Tôi đồng ý: loại hết khung tối sẽ mất dữ liệu ban đêm/đường hầm thật.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2** (mặc định pipeline): đủ để loại khung gần trùng do camera đứng hình/xe chạy chậm mà không xoá nhầm khung khác nhau thật; ngưỡng 6 loại quá nhiều (hơn nửa số khung), ngưỡng 0 giữ lại quá nhiều khung gần trùng. Ngưỡng phụ thuộc tốc độ xe: xe chạy chậm làm các khung liên tiếp gần như giống hệt nên khoảng cách dHash nhỏ hơn, vì vậy một ngưỡng theo tốc độ sẽ chính xác hơn một ngưỡng cố định.

## Bước 4 · Gold

### Câu 6
Theo `leakage.json`: chia **theo khung hình** làm **cả 26/26 trip** bị chia cắt và 26 trip có khung ở cả train lẫn test; chia **theo trip** thì **0** trip bị chia cắt. Vì các khung liền nhau gần như giống hệt nhau, chia theo khung khiến các bản gần trùng của khung test lọt vào train – mô hình "đã thấy đề thi", nên điểm kiểm thử đẹp một cách giả tạo và không phản ánh khả năng tổng quát hoá.

### Câu 7
`dataset_version` là **băm nội dung** (12 ký tự hex) trên chính các khung, cách chia split và lineage, chứ không phải ngày giờ hay số đếm – nhờ vậy nó bất biến theo thời gian chạy nhưng đổi ngay khi nội dung đổi. Hai lần chạy liên tiếp cho **cùng một version** `b4fa2af6fbed`. Version của tôi **khác** bạn bè dù cùng dữ liệu vì `LAB_STUDENT_ID` được trộn vào thứ tự md5 dùng để xếp trip vào train/val/test, nên cách chia split (và do đó nội dung) khác nhau.

### Câu 8
Phần `lineage` trong `manifest.json` cho biết dataset được tạo từ: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.frames` v50, cùng `input_video_sha256` của drive_a `636ba1a9...` và drive_b `bfa62703...`. Nghĩa là nó dựng từ telemetry/trips bronze phiên bản 0, danh mục video phiên bản 1, bảng khung silver phiên bản 50, và hai video gốc theo sha256. Nếu ba tháng sau cần tái tạo đúng dataset này, chỉ cần các số phiên bản Delta (time travel), sha256 video đầu vào, luật lọc (`drop: is_black, is_blurry, is_near_dup, not sync_ok`) và cách chia `per trip, stratified by drive, md5 order` cùng MSSV.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Khi bật `LAB_FAIL_ONCE=trip-01037`, đúng **một** task `silver_frames` (mapped, `map_index=2`) chuyển **đỏ** – log ghi `RuntimeError: simulated crash while extracting trip-01037; run it again`; các task phía sau chuyển `upstream_failed`. Tôi **chỉ clear task đỏ đó**; nó chạy lại và đạt, DAG chuyển xanh. Dữ liệu **không nhân đôi** (Delta overwrite theo partition) và `dataset_version` vẫn `b4fa2af6fbed`; `verify` vẫn **27/27**. Bằng chứng: ảnh log task đỏ (thấy trip-01037) và ảnh Graph xanh sau khi chạy lại.

### Câu 10
Nâng lên 500 xe/ngày, ba thành phần vỡ đầu tiên: (1) **điều phối + compute cục bộ** – một Airflow LocalExecutor trên một máy không đủ song song cho 500 xe × hàng nghìn khung, cần executor phân tán (Celery/Kubernetes) và engine phân tán (Spark/Trino) theo slide 22–23; (2) **lưu trữ/warehouse đơn nút** – MinIO một nút và Postgres một máy thành nút cổ chai throughput/độ sẵn sàng, cần object storage cụm và warehouse mở rộng (Snowflake/BigQuery/Redshift) theo slide 16; (3) **giải mã video** – mỗi trip một task FFmpeg và quét lại file gốc là nút cổ chai, cần ingestion tăng dần/hướng sự kiện, worker tự mở rộng và backfill theo slide 12, 22.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL tôi thêm vào dashboard – tỷ lệ khung bị loại theo từng trip:

```sql
select trip_id, split, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped
from marts.mart_trip_summary
order by pct_dropped desc
```

Nó cho biết trip nào có tỷ lệ khung bị loại cao nhất và mức độ, giúp khoanh vùng trip có vấn đề camera/chất lượng (khung đen, mờ, trùng, mất đồng bộ) thay vì chỉ nhìn con số gộp toàn cục.

## Nhìn lại

Bất ngờ nhất là cùng một bảng `silver.frames` có thể vừa idempotent (chạy lại không đổi) vừa bắt được lỗi cấy sẵn mà không làm hỏng dataset có phiên bản. Điều còn chưa rõ là ngưỡng trùng dHash cố định (2) có còn đúng khi tốc độ xe thay đổi mạnh giữa các trip. Một lỗi tôi nghĩ pipeline **chưa** bắt chắc: telemetry "trôi" dần (drift) nhưng vẫn nằm trong khoảng 0..70 m/s – ví dụ tốc độ bị lệch hệ thống một lượng nhỏ nhưng nhất quán – sẽ qua được `in_range` và chỉ hiện ra như sai số, không bị quarantine.
