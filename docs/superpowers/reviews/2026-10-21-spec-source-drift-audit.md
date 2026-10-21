# Audit lệch spec/plan ↔ source — WebRTC Signaling v1.11

Ngày: 2026-10-21 · Branch `dev-docker-fix` @ `9cc8e41` (checkout sạch trước audit).

## Phạm vi và giới hạn

Đối chiếu spec v1.11 (§§3–12, 24–27, 37, 45–49 đọc trực tiếp; các phần còn lại kiểm bằng grep theo hằng số/hành vi normative), plan 24 task, 5 plan Minikube, spec Identity/Minikube và các báo cáo audit trước (2026-10-09/12) với **source hiện tại**.

- Đã chạy: `./mvnw -o test-compile` → **BUILD SUCCESS** 13 module (cả test source); `spotless:check` → **FAIL** (9 file theo log, đầu tiên là `MediaTelemetry.java`; CLAUDE.md ghi "~40 file" nên con số đó đã cũ).
- **Không** chạy full `verify` (≈18 phút, cần Docker/Chromium) nên không xác nhận lại "770 test PASS" của ledger 2026-10-12. Không chạy benchmark/deploy.
- Đây là khảo sát có mục tiêu, không phải chứng minh đầy đủ mọi call chain. Mức độ: **Cao** = vi phạm contract normative / thiếu cơ chế bắt buộc; **Vừa** = lệch có hệ quả vận hành; **Thấp** = lệch tài liệu/hygiene.

## 1. Các finding của audit 2026-10-09 (A01–A15): đã đóng trong source chưa?

Kiểm lại trên source hiện tại, không dựa vào ledger:

| Mục | Kết quả kiểm |
|---|---|
| A01 job bắt buộc | **Đã đóng**: [NativeRuntimeStartup.java:54](../../../signaling-app/src/main/java/io/webrtc/signaling/app/NativeRuntimeStartup.java:54) yêu cầu `installedJobs(NativeActorMaintenance.REQUIRED_JOBS)`. |
| A02 outbox starvation | **Đã đóng ở mức source**: claim theo `ORDER BY next_attempt,event_id`, không còn tầng ưu tiên hint ([OutboxRepository.java:219](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/OutboxRepository.java:219)). |
| A06 N/N−1 gate | **Đã nối runtime**: `CompatibilityWindow` được `NativeCompatibilityEnrollment/Collectors` sử dụng. Rolling upgrade thật vẫn chưa có (xem F11). |
| A03, A04, A05, A08–A15 | Ledger báo "source accepted"; tôi không kiểm lại từng mục. Mọi mục đó vẫn chưa có evidence runtime/deploy. |
| A07 identity attestation | **Vẫn mở** theo đúng thiết kế (fail-closed, `REQUIRED_UNCONFIGURED`). |

Kết luận: phần lớn gap kiến trúc lớn của audit trước đã được xử lý trong code; những gì còn lại tập trung ở hai nhóm bên dưới.

## 2. Finding mới — lệch spec so với source

### F1 — Timer nghiệp vụ của call hard-code trong SQL service, không phải typed config (**Cao**, §49.8)

> **Trạng thái 2026-10-21: đã sửa** — `CallTimers` (storage) + `signaling.calls.*` trong `production-defaults.yaml`/schema, đi vào fingerprint; mặc định giữ đúng giá trị cũ; cấu hình chỉ được rút ngắn, không kéo dài cửa sổ authority. Token bucket gateway nay lấy từ `signaling.rate-limits.*`.

§49.8: mọi hằng số production phải là typed config có validated range và owner; "startup rejects unsafe configuration". Thực tế các timer sau là literal trong code, không có trong `production-defaults.yaml`, `SignalingProperties` hay JSON schema:

| Timer | Vị trí |
|---|---|
| RINGING 30s | [CallWorkflowService.java:195](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/CallWorkflowService.java:195) |
| Activation 5s | [CallWorkflowService.java:196](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/CallWorkflowService.java:196) |
| Reconnect grace 75s | [CallWorkflowService.java:155](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/CallWorkflowService.java:155) |
| Negotiation 20s | [CallCommandService.java:553](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/CallCommandService.java:553) |
| Media recovery window 30s, debounce 5s, tối đa 1 lần restart | [CallCommandService.java:817-830](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/CallCommandService.java:817) |
| Token bucket gateway 40/60, 20/s, 30/s; in-flight 64/8/56; 256 KiB | [FrameAdmissionHandler.java:63-84](../../../signaling-gateway/src/main/java/io/webrtc/signaling/gateway/FrameAdmissionHandler.java:63) |

Hệ quả: fingerprint cấu hình **không bao phủ** các giá trị này nên hai build khác timer vẫn cùng fingerprint, trái §49.8 ("config fingerprints… mixed fleets detectable"). `MediaRecoveryPolicy` nhận window/attempts qua constructor (tối đa 3 lần, ≤75s) nhưng đường authority SQL cố định 1 lần/30s, tức hai nguồn sự thật không khớp nhau.

### F2 — Không có rate limiting đúng nghĩa; `RATE_LIMITED` không bao giờ được phát (**Cao**, §49.7, §10)

> **Trạng thái 2026-10-21: đã sửa phần gateway** — bucket per-(issuer,user) dùng chung giữa các session (`UserRateLimiter`, `UserRateLimitHandler`), từ chối bằng `ERROR/RATE_LIMITED` + `retryAfterMs`, không đóng socket; lệnh critical không bị giới hạn bởi bucket này. **Còn mở:** rate limit cho bootstrap API.

`ErrorCode.RATE_LIMITED` chỉ xuất hiện ở enum; không có producer nào trong `src/main`. Gateway có per-IP admission ([EdgeAdmission.java](../../../signaling-gateway/src/main/java/io/webrtc/signaling/gateway/EdgeAdmission.java)) và token bucket theo **socket**, nhưng khi vượt token thì **đóng socket** (`ctx.close()`), còn khi vượt budget thì trả `OVERLOADED`. Chưa có giới hạn **per-user/per-session** (§49.7 "per-user/session limits") hay giới hạn ở bootstrap API, và client không nhận mã `RATE_LIMITED` để áp dụng backoff theo Retry-After.

### F3 — Không có circuit breaker cho destination cell và retry budget 10% (**Vừa**, §27)

Không có `breaker`/`half-open`/retry-budget nào trong `signaling-cell-rpc/src/main`. Có class-isolated credits và deadline, nhưng thiếu cơ chế normative: mở breaker sau ≥20 lần thử và >50% lỗi tạm thời trong cửa sổ 10s, half-open sau 5s với ≤3 probe, retry ≤10% capacity. Đây là "proposed starting breaker" nên là Vừa, nhưng `RpcRetryBackoff` chỉ giới hạn số lần trên mỗi operation, không giới hạn tổng retry của destination.

### F4 — Ping WSS không có jitter (**Vừa**, §6 và §37)

Spec: "ping every 30 seconds with jitter… không schedule mọi socket cùng một giây". [HeartbeatHandler.java:96-99](../../../signaling-gateway/src/main/java/io/webrtc/signaling/gateway/HeartbeatHandler.java:96) đặt `nextPing = now+30s` cố định; không có jitter trong module gateway. Sau một đợt reconnect storm (sau AZ loss, drain), các socket đồng pha sẽ ping đồng loạt mỗi 30s.

### F5 — Không enforce thời lượng call tối đa 8h (**Vừa**, §8.4)

[DurableDeadlines.java:18-23](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/DurableDeadlines.java:18) không có nhánh `ESTABLISHED`/`CONNECTING` ngoài `negotiationUntil`; không có "8 hours" ở bất cứ đâu. Spec dùng chữ "proposed", nhưng nếu giữ thì cần timer; nếu bỏ thì cần ADR cập nhật spec. Hiện một call ESTABLISHED chỉ kết thúc bởi hangup, mất lease hoặc media failure.

### F6 — Client tham chiếu không có reconnect backoff (**Vừa**, §11)

`clients/native-browser` chỉ có `attachWebSocket/detachWebSocket`; không có full-jitter backoff 0.5–30s, không xử lý `Retry-After`, không có luồng "RESUME rồi SYNC_CALL". Gateway có phát `RECONNECT retryAfterMs`. Chỉ `signaling-loadgen` (`ReconnectBackoff`) cài hành vi này. README của client nên nêu rõ phần này nằm ở app host, nếu không đây là gap acceptance của Task 17.

### F7 — Gateway không tách `TURN/ICE` khỏi signaling ở metric/SLI (**Vừa**, §49.4)

Mã reason `ICE_FAILED/TURN_UNAVAILABLE/STUN_UNAVAILABLE` được validate ([ProtocolValidator.java:279](../../../signaling-protocol/src/main/java/io/webrtc/signaling/protocol/ProtocolValidator.java:279)) nhưng không có metric riêng phân tách "signaling OK / media degraded" trong `SignalingMetrics`. Spec yêu cầu SLI báo cả hai chiều.

### F8 — `idle_in_transaction_session_timeout` dùng budget thay vì 1s (**Thấp**, §24.2)

[SqlTransactions.java:116](../../../signaling-storage/src/main/java/io/webrtc/signaling/storage/SqlTransactions.java:116) đặt bằng `ms` (budget lệnh, tới 2s); spec đề xuất 1s. `lock_timeout` ≤100ms và `transaction_timeout` ≤2s đúng spec.

### F9 — Bộ mã lỗi công khai lệch spec (**Thấp**, §10)

`ErrorCode` có thêm `INVALID_FRAME`, `UNSUPPORTED_PROTOCOL`, `SECURITY_REVOKED` ngoài 20 mã được liệt kê. Là thay đổi additive hợp lý nhưng spec/phiên bản protocol cần cập nhật để client N−1 biết xử lý mã lạ.

## 3. Gap triển khai và tài liệu

### F10 — Kế hoạch Minikube chưa bắt đầu (**Cao** nếu mục tiêu của nhánh `dev-docker-fix` là chạy trên Minikube)

Spec Minikube trạng thái `APPROVED_FOR_PLANNING`; 4 plan con có **0/114** checkbox hoàn thành (40 + 20 + 32 + 22); plan tổng không có task. `deploy/compose/` là thư mục rỗng. Chart `deploy/helm/signaling` chỉ có gateway/actor/control, chưa có edge SNI router, identity, regional control hai scope, source service/DB, observability. Chỉ plan Identity (23/23) đã làm xong. Mọi lệnh "Minikube" trong tài liệu hiện chưa thực hiện được.

### F11 — Task 20/21/23/24 và 7 nghĩa vụ ngoài vẫn mở (**Cao theo plan, đã được ghi nhận**)

- `rolling-upgrade.sh` và `pitr-restore.sh` chỉ có `--dry-run` (chạy unit test local); không có drill thật dù plan Task 21 Step 4 viết "PASS in the integration environment".
- Bảy nghĩa vụ fault ngoài (three-AZ partition, primary-clock uncertainty, revocation thật, PG primary failure, AZ loss, mixed-version rollback, restore) vẫn `NOT_RUN`.
- Chưa có candidate deploy được; 3 thư mục evidence là `NOT_QUALIFIED`. Trạng thái release đúng với CLAUDE.md: **NOT_QUALIFIED**.

### F12 — Mâu thuẫn giữa spec Identity, spec Minikube và CLAUDE.md về key/credential (**Thấp–Vừa**)

`signaling-identity/src/main/resources/keys/private.pem` và mật khẩu `admin123`/`mod123` trong `application.yml` được tracked. Việc này **được spec Identity cho phép rõ ràng** (spec dòng 8 và 66: key và account plaintext chỉ cho dev/test). Nhưng CLAUDE.md cấm credential trong file tracked, còn spec Minikube thay yêu cầu bằng Secret-mounted key và nói image không chứa private key. Ba tài liệu cần thống nhất: hoặc ghi ngoại lệ `TEST_ONLY` cho module identity trong CLAUDE.md, hoặc thực hiện thay thế trước khi build image identity (`Dockerfile` hiện chỉ đóng gói `signaling-app`).

### F13 — Hygiene tài liệu/CI (**Thấp**)

- `.github/` bị `.gitignore` nên không có CI nào được version; spec §49.9 yêu cầu dependency/CVE scan và gate lặp lại.
- CLAUDE.md nói spotless fail "~40 file", thực đo 9 file; cập nhật hoặc chạy `spotless:apply` theo từng module.
- CLAUDE.md ghi "Task 23–24 open", trong khi code `signaling-loadgen` đã ~4.000 LOC và có 3 bundle evidence; nên ghi rõ phạm vi "harness có, chạy thật chưa".
- Plan gốc có checkbox `[ ]` cho Task 1–21, 23, 24 dù nhiều task đã hoàn thành (chỉ Task 22 được đánh dấu); ledger nói "checkbox không phải evidence", nên thực tế không có nguồn trạng thái duy nhất.

## 4. Đã khớp spec (kiểm trực tiếp)

Cấu hình `production-defaults.yaml` (15s/5s lease, SBR 10s/10s, frame 80 KiB, JWT 8 KiB, SDP 64 KiB, ICE 2 KiB/20/8 KiB/256 KiB, AUTH 5s, pong 10s, RPC 2s/1s, queue 64/256 KiB/128) · `application-actor.conf` (ddata, `remember-entities=off`, 1024 shard, no Java serialization, SBR keep-majority + down-all-when-unstable, buffer 256, handoff 10s, Artery limits 256 KiB/256/256/512/16, TLS1.3 mTLS, dispatcher throughput 32/5ms, mailbox 128) · endpoint `POST /v1/signaling/bootstrap` · enum trạng thái và lệnh (REJECT, DECLINE_ALL, NEGOTIATE_REQUEST, RESUME, SYNC_CALL, GET_COMMAND_RESULT) · `AUTH_EXPIRING` 60s · retention 24h (+5s) trong SQL · relay 30s/ICE 10s · 18 migration cho toàn bộ bảng §12 (cộng bảng recovery/transfer) · Terminal call retire sau 2 phút.

## 5. Thứ tự xử lý đề xuất

1. F1 (đưa timer vào typed config + fingerprint) và F2 (rate limit per-user + mã `RATE_LIMITED`) — đều đụng contract chung nên làm cùng một đợt.
2. F4, F6, F5, F3 — độc lập, nhỏ; F4 trước vì ảnh hưởng reconnect storm.
3. F10 nếu Minikube là mục tiêu của nhánh hiện tại; F12 trước khi build image identity.
4. F11 chỉ đóng bằng evidence thật (Task 23–24), không bằng thay đổi code.

Mọi thay đổi chạm authority/fencing cần chạy `impact` GitNexus theo CLAUDE.md trước khi sửa.
