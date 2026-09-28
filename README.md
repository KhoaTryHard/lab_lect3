# Logical Clock Simulator

Đây là bài lab mô phỏng đồng hồ logic cho môn Hệ thống phân tán. Chương trình chạy ba worker thread `P0`, `P1`, `P2`, mỗi worker sở hữu một Lamport clock và một vector clock kích thước 3. Worker chỉ giao tiếp bằng `queue.Queue`; không có clock dùng chung giữa các worker.

## Chạy chương trình

Từ thư mục dự án:

```powershell
python -m logical_clock_simulator --mode demo --output-dir output-demo
python -m logical_clock_simulator --mode random --steps 30 --seed 42 --output-dir output-random
python -m unittest discover -s tests -v
```

Mỗi lần chạy ghi `events.log`, `events.jsonl` và `summary.json` vào thư mục output. Chế độ `demo` dùng sáu lệnh xác định để đối chiếu rubric. Chế độ `random` cho mỗi worker thực hiện số lượt thử được chỉ định; một lần RECEIVE khi inbox rỗng chỉ ghi `RECV SKIPPED` và không tăng clock.

## Kiến trúc

```text
                 command queues (main -> worker)
       ┌──────────────┬──────────────┬──────────────┐
       ▼              ▼              ▼
    Worker P0       Worker P1       Worker P2
    L0, V0          L1, V1          L2, V2
       │ inbox 0       │ inbox 1       │ inbox 2
       └───────────────┴───────────────┴──────────────┐
                                                       ▼
                                      output queue (worker -> main)
```

Main gửi command qua ba queue điều khiển và nhận event/status qua một output queue. Message dữ liệu đi vào inbox của worker đích. Khi mọi random command hoàn tất, main yêu cầu từng worker drain inbox rồi mới gửi STOP.

## Quy tắc clock

- LOCAL: `Lamport += 1`, sau đó `V[pid] += 1`.
- SEND: cập nhật như LOCAL một lần; message giữ bản sao Lamport/vector sau cập nhật.
- RECEIVE: lưu vector cục bộ trước khi nhận để kiểm tra quan hệ; sau đó `Lamport = max(local, message) + 1`, merge `max` theo từng thành phần và tăng `V[receiver]` một lần.

Hai vector concurrent khi không vector nào nhỏ hơn hoặc bằng vector kia theo mọi thành phần. Kiểm tra concurrency xảy ra trước merge; nếu kiểm tra sau merge thì conflict sẽ bị che mất.

Ví dụ demo có các trạng thái chính:

```text
[P0] LOCAL EVENT   | Lamport: 1 | Vector: [1, 0, 0]
[P0] SEND MSG to P1 | Lamport: 2 | Vector: [2, 0, 0]
[P1] RECV MSG fr P0 | Lamport: 3 | Vector: [2, 1, 0]
[P2] LOCAL EVENT   | Lamport: 1 | Vector: [0, 0, 1]
[P1] SEND MSG to P2 | Lamport: 4 | Vector: [2, 2, 0]
[P2] CONFLICT DETECTED with P1! Vector [0, 0, 1] || [2, 2, 0]
[P2] RECV MSG fr P1 | Lamport: 5 | Vector: [2, 2, 2]
```

## Đối chiếu rubric

`core.py` chứa quy tắc Lamport, merge vector và bốn quan hệ `BEFORE`, `AFTER`, `EQUAL`, `CONCURRENT`. `simulator.py` chứng minh ba thread thật, queue message-passing, drain/stop có giới hạn thời gian và log text/JSONL. `tests/test_core.py` kiểm tra từng công thức; `tests/test_simulator.py` kiểm tra demo, conflict, drain, vòng đời worker và file log.

Giá trị `[2, 2, 0]` ở message P1 -> P2 là chủ ý: SEND là một event nên phải tăng vector của P1 từ `[2, 1, 0]`. Một ví dụ trên slide rút gọn bước này; mã nguồn tuân theo quy tắc chính thức trong bài giảng và giáo trình.
