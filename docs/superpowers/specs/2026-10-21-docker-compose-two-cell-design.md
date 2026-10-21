# Docker Compose hai cell — môi trường signaling chạy được trên một máy

Date: 2026-10-21
Status: DRAFT_FOR_REVIEW — thiết kế đã được người dùng duyệt ở mức hội thoại (2026-10-21); spec bằng văn bản chờ rà soát. Implementation và deployment NOT_RUN.
Environment: **LOCAL_TEST_ONLY** / `local-minikube` / `local-test-only`.
Qualification: **NOT_QUALIFIED**. Compose chỉ có thể đạt `LOCAL_QUALIFIED`. Không có tuyên bố production, HA thật, physical fencing hay 10M.
Source baseline: branch `dev-docker-fix`, `9c14b4f`.

## 1. Mục tiêu và phạm vi

Chạy được luồng `Login → JWT RS256 → Bootstrap → Directory resolve → WSS → Gateway → Actor RPC → Pekko actor → PostgreSQL → cross-cell signaling` trên một máy bằng `docker compose up`, với hai home cell `c001`, `c002`, mỗi cell có Pekko cluster, cell PostgreSQL và gateway riêng, cùng một control, Directory PostgreSQL, Identity và source service dùng chung. SDP/ICE chỉ là signaling metadata; media không đi qua hệ thống.

Hiện trạng làm cho mục tiêu này không đạt được chỉ bằng file compose (đã xác minh bằng đọc source ngày 2026-10-21):

- Mỗi plane chỉ lắp ráp khi có bean `Native*Enrollment`; trong `src/main` chỉ `NativeCompatibilityCollectors` tạo một enrollment, phần còn lại do test đăng ký. Image hiện từ chối khởi động với `NATIVE_RUNTIME_NOT_INSTALLED` (`qualification/scenarios/image-contract.sh`).
- `MountedNativeContract` chỉ parse và validate, không tạo bean.
- Không có source service cho đồng hồ và revocation đã ký, không có bộ sinh PKI, không có công cụ migrate/seed, và Pekko discovery bắt buộc `kubernetes-api`.

Vì vậy spec này gồm ba dự án con, mỗi dự án có plan triển khai riêng:

| # | Dự án con | Nơi chứa | Nguồn tái dùng |
|---|---|---|---|
| SP1 | Local runtime adapter | `signaling-app/.../runtime` | `docs/superpowers/plans/2026-10-13-minikube-authority-runtime.md` A5/A6 |
| SP2 | Local platform (source service) | module mới `signaling-local-platform` | cùng plan, A1 và phần rút gọn A2 |
| SP3 | Môi trường compose | `deploy/compose/` | spec này |

Spec này đặc tả SP3 đầy đủ và **hợp đồng giao tiếp** giữa ba phần. Thứ tự triển khai: SP1 → SP2 → SP3.

### Không thuộc phạm vi

A3/A4 (transfer qua mTLS giữa các control), A7 (health TLS), A8 (trace export), JWKS fetcher và rotation, edge SNI router, Prometheus/Grafana, media/TURN. Control giữ credential của cả hai cell DB vì `NativeBucketTransferEnrollment` hiện yêu cầu hai `SqlTransactions` trong một process; điều này vi phạm tách quyền của spec Minikube §4.1 và được chấp nhận chỉ trong `LOCAL_TEST_ONLY`.

## 2. Quyết định

| Chủ đề | Quyết định | Lý do / đánh đổi |
|---|---|---|
| Phạm vi cell | Hai cell `c001`, `c002` | Chạy được cross-cell saga và bucket-transfer owner thật (cần hai cell khác nhau). Đắt gấp đôi một cell. |
| Durability | Cell DB: primary + 1 standby streaming, `synchronous_standby_names='ANY 1 (...)'`; Directory và Source: một node | Chứng minh nhánh commit chờ standby; không dùng `localSingleWriter`. Tránh hai standby không cần thiết. |
| Pekko discovery | Fake kube-API sidecar | Không nới validate production (`ShardingBootstrap.validateProduction`). |
| Identity | Giữ nguyên `signaling-identity` và key test đã commit; verifier dùng `jwtPublicKeys` tĩnh | Spec Identity cho phép; không cần JWKS fetcher. |
| Edge | Publish cổng trực tiếp, hostname qua `/etc/hosts` | Không có SNI router; gateway tự kết thúc TLS. |
| Health | HTTP trên IP container, cổng 8559 | A7 ngoài phạm vi. |

Phương án đã loại: main class local riêng dựng enrollment kiểu test (không dùng lại được cho Minikube); chỉ viết compose giả định launcher có sẵn (không chạy được); `seed-nodes` tĩnh trong local mode (nới validate an toàn).

## 3. Topology

### 3.1 Container (24)

| Nhóm | Container | Ghi chú |
|---|---|---|
| Mỗi cell (×2) | `actor-<cell>-1..6` | Cố định 6 (`SignalingProperties.Cluster` yêu cầu), quorum 4, `required-contact-point-nr=6` |
| | `gateway-<cell>` | WSS 8443, relay gRPC 9443 |
| | `pg-<cell>-primary`, `pg-<cell>-standby` | PostgreSQL 17.6 |
| Dùng chung | `control` | HTTPS 8443, giữ credential cả hai cell DB |
| | `identity` | HTTP 8081 |
| | `local-platform` | Source service, HTTPS/mTLS 9443 |
| | `kube-api` | Fake Kubernetes API cho Pekko |
| | `pg-directory` | Hai database: `directory`, `source` |
| | `provisioner` | One-shot, hai giai đoạn |

Ước tính tài nguyên: 14 JVM × 512 MB + 5 PostgreSQL ≈ 10 GB RAM. Đây là ước lượng, không phải đo; kiểm trong plan.

### 3.2 Mạng và địa chỉ tĩnh

Discovery dùng `use-raw-ip=true` và management client kiểm hostname bằng thuật toán `HTTPS`, nên mọi actor phải có IP cố định và certificate có IP SAN.

| Mạng | Subnet | Chứa | Dùng cho |
|---|---|---|---|
| `net-c001` | 172.30.1.0/24 | actor c001 (.11–.16), `pg-c001-primary` (.101), `pg-c001-standby` (.102), `kube-api` (.250) | Artery 25520, management 8558, DB, discovery |
| `net-c002` | 172.30.2.0/24 | tương tự (.11–.16, .101, .102), `kube-api` (.250) | như trên |
| `net-shared` | 172.30.0.0/24 | actor c001 (.31–.36), actor c002 (.41–.46), `gateway-c001` (.21), `gateway-c002` (.22), `control` (.10), `identity` (.11), `local-platform` (.12), `pg-directory` (.14) | RPC 8443/9443, bootstrap, source |

Artery và management chỉ lên `net-<cell>`, nên c001 không có đường tới Artery của c002. `control` nối cả `net-c001`, `net-c002` và `net-shared` để tới ba PostgreSQL. `kube-api` nối cả hai mạng cell và phục vụ mỗi cell qua IP riêng của nó. Địa chỉ cụ thể nằm trong `topology.env` và là nguồn duy nhất cho compose, generator PKI và contract.

Cổng publish ra host: gateway c001 `8443`, gateway c002 `8444`, control `8445`, identity `8081`. Hostname `c001.signaling.local`, `c002.signaling.local`, `bootstrap.signaling.local`, `identity.signaling.local` trỏ về `127.0.0.1` qua `/etc/hosts` (script in dòng cần thêm, không tự sửa file hệ thống).

## 4. Hợp đồng giữa SP1, SP2, SP3

### 4.1 Contract mount (SP3 sinh, SP1 đọc)

Dùng `MountedNativeContract` schema v1 (`qualification=LOCAL_TEST_ONLY`, `environment=local-test-only`), nạp bằng `load(file, "local-minikube", "LOCAL_TEST_ONLY")`. Mỗi container có thư mục `/run/signaling/` (read-only, mode 0440, group 10001) chứa `provider.json`, `runtime.yaml` và mọi file mà contract tham chiếu. Ràng buộc đã có trong parser và phải được generator tôn trọng:

- File ≤ 64 KiB; mọi đường dẫn tuyệt đối, dưới thư mục chứa contract, ≤ 512 ký tự.
- JDBC URL chứa `sslmode=verify-full`.
- `cells[]` chứa cell của chính nó với cùng `storageEpoch`; `gateways[]` không rỗng; `proofs.publicKeys` chứa khóa `"<cell>/<keyId>"` của chính nó.
- `directorySnapshotSha256` là SHA-256 hex của `directorySnapshot`.
- `identity.issuer/audience` khớp `SIGNALING_IDENTITY_ISSUER/AUDIENCE`; `maximumJwtLifetime` ≥ 15 phút; `revocationPropagationSLO < hardSafetyBound < maximumJwtLifetime`; `clockSkew` ≤ 30s; `preserveJtiOnRefresh=true`.

`runtime.yaml` chỉ chứa các section `signaling` có kiểu (`identity`, `lease`, `cluster`, `protocol`, `transport`, `queues`, `calls`, `rate-limits`), một root, scalar-only.

### 4.2 Biến môi trường mỗi container

`SPRING_PROFILES_ACTIVE` (đúng một trong `gateway|actor|control`), `SIGNALING_DEPLOYMENT_MODE=local-minikube`, `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY`, `SIGNALING_CELL_ID`, `SIGNALING_CLUSTER_FINGERPRINT` (64 hex, giống nhau trong cell), `SIGNALING_IDENTITY_ISSUER`, `SIGNALING_IDENTITY_AUDIENCE`, `SIGNALING_RUNTIME_CONTRACT=/run/signaling/runtime.yaml`. Riêng actor: `SIGNALING_POD_IP`, `SIGNALING_NAMESPACE`, `SIGNALING_ACTOR_SERVICE=signaling-<cell>`, `SIGNALING_DISCOVERY_SELECTOR=app=webrtc-signaling,plane=actor,cell=<cell>`, `SIGNALING_REMOTING_KEYSTORE`, `SIGNALING_REMOTING_TRUSTSTORE` và ba biến mật khẩu (`SIGNALING_REMOTING_KEYSTORE_PASSWORD`, `SIGNALING_REMOTING_KEY_PASSWORD`, `SIGNALING_REMOTING_TRUSTSTORE_PASSWORD`) lấy từ file mount, không đặt trong file compose.

`SIGNALING_POD_UID` (UUID cố định mỗi container, sinh bởi provisioner) và `SIGNALING_FORMATION_PROFILE` hiện không có mã Java nào đọc; SP1 phải tiêu thụ chúng. `processBoot` luôn được sinh mới trong JVM và đăng ký với source service trước khi lấy báo cáo đồng hồ.

### 4.3 Nhiệm vụ SP1 — Local runtime adapter

Kích hoạt chỉ khi cả ba dấu hiệu local có mặt (mode, acknowledgement, environment); image production vẫn từ chối contract local. Adapter phải:

1. Dựng và đăng ký các enrollment hiện có cho từng plane (danh sách ở `docs/superpowers/plans/2026-10-13-minikube-authority-runtime.md` A6): gateway business/ingress/lifecycle/cell-RPC; actor process/source/business/ingress/scheduling/maintenance/WAL/compatibility; control database/source/business/ingress/bucket-transfer.
2. Dựng `SqlTransactions`, `DbBoundary`, `DbPools` cho actor từ `cellDatabase`; chưa có mã nào làm việc này.
3. Dựng `BoundedTokenVerifier` từ `jwtPublicKeys` (`TrustedRsaKeys` + `Rs256TokenVerifier`) cho mọi plane.
4. Dựng Pekko `Config` cho actor: nạp `application-actor.conf`; vai trò `az-a`/`az-b` (3+3 mỗi cell, `CompatibilityWindow` cần ≥ 2 vùng); các khóa `signaling.compatibility.{major,minor,executable-id,features}` và `signaling.compatibility.window.*`; override discovery (`api-ca-path`, `api-token-path`, `api-service-host-env-name`, `api-service-port-env-name`); keystore/truststore PKCS12.
5. Đăng ký `ClusterReadiness.localMinikube("LOCAL_TEST_ONLY")`, thay bean mặc định.
6. Dựng nguồn đồng hồ/revocation phía client (`NativeClockSource`, `NativeCachedRevocationSource`, `NativeRevocationSource`) cho gateway và control; chỗ này chưa có wiring cho hai plane đó.
7. Dựng `NativeCompatibilityEnrollment` từ quan sát thật; control cần `CellRpcClient` hoặc enrollment tương thích riêng.
8. Dựng `NativeBucketTransferEnrollment` với hai store thuộc hai cell khác nhau (`c001`, `c002`).

Không đưa `TestOnlyNativeCapabilities`, đồng hồ giả, WAL fact giả hay readiness vô điều kiện vào image triển khai.

### 4.4 Nhiệm vụ SP2 — `signaling-local-platform`

Server HTTPS/mTLS cổng 9443, TLS 1.3, chỉ nhận caller đã enroll; tuân theo wire contract trong `config/native-sources.md`: header `X-Signaling-Pod-Uid`, `X-Signaling-Process-Boot`, `X-Signaling-Source-Offset`; `Content-Length` đúng, `Connection: close`, không redirect/chunked/nén, body ≤ 32768 byte.

- **Đồng hồ**: báo cáo ký Ed25519 trên `ClockSafetyMonitor.signingBytes`, hiệu lực ≤ 5s, `pairUncertainty` ≤ 250000µs, `relativeRateError` ≤ 1000 ppm. Số liệu đến từ đo thật (lệch giữa wall clock và monotonic clock qua các mẫu liên tiếp); không có đo thì trả trạng thái không an toàn, không bao giờ hằng số an toàn. Giới hạn của phép đo (chỉ thấy đồng hồ của container, không phải kỷ luật thời gian của host) được in vào evidence.
- **Revocation**: log append-only bền trong database `source` của `pg-directory`, offset và high-water đơn điệu, mỗi trang ≤ 16 mục (sự kiện + khóa bị loại), ký bằng `RevocationSourceVerifier.signingBytes`. Endpoint admin (mTLS, vai trò riêng) để thêm revoke user/token.
- `registerBoot(podUid, processBoot, cell, plane)` gắn với danh tính certificate; boot mới thay boot cũ của Pod đó; header không tự đăng ký được danh tính.
- Chạy được độc lập, yêu cầu `LOCAL_TEST_ONLY` trước khi khởi động; không nằm trong auto-configuration của `signaling-app`.

### 4.5 Cơ sở dữ liệu (SP3 sinh, SP1 kiểm)

- **Cell DB** (`signaling`): Flyway `signaling-storage/src/main/resources/db/migration/V001–V018` với bảng lịch sử mặc định `flyway_schema_history` (cổng tương thích so checksum V016 với CRC32 của file). App không chạy Flyway (bị loại khỏi auto-config).
- **Directory DB** (`directory`): `signaling-control-api/src/main/resources/db/directory/migration/V001–V002` với bảng lịch sử riêng. **Source DB** (`source`): migration của `signaling-local-platform`, tách khỏi mọi bảng cell.
- **Seed cell**: `cell_authority(1,'<cell>',1,'GROUPED',1,'ACTIVE')`; `bucket_authority` đủ 16384 hàng cho bucket 0–16383 (`directory_epoch=1`, `recovery_epoch=1`): bucket được gán cho cell đó có `status='ACTIVE'`, các bucket còn lại `status='FROZEN'` (không phục vụ nhưng có hàng sẵn cho transfer); phép chia bucket giữa hai cell phải trùng với seed directory; `group_owner` cho nhóm 0–1023 (`ownership_hash_version=1`, `storage_epoch=1`, `group_epoch=1`, `lease_sequence=0`, `IDLE`).
- **Seed directory**: 16384 dòng `directory_bucket`, chia đôi giữa `c001` và `c002`, `wss_url` trỏ `wss://c001.signaling.local:8443/ws` và `wss://c002.signaling.local:8444/ws`.
- **WAL**: role replication, slot hoặc `application_name` cố định cho standby; provisioner chờ standby `sync` trong `pg_stat_replication`, đọc `systemIdentifier` và timeline, ghi vào `NativeWalEnrollment` (không dùng `localSingleWriter`). Cần quyền đọc `pg_stat_replication`, `pg_control_system()` và `pg_current_wal_flush_lsn()`; provisioner cấp bằng `pg_monitor` (quyền chính xác xác minh trong plan).
- PostgreSQL ≥ 17 vì `SET LOCAL transaction_timeout`. Image `postgres:17.6` đúng như pin trong `config/compatibility-manifest.yaml`.

### 4.6 PKI

Generator thuần `openssl` + `keytool`, deterministic theo `topology.env`, ghi vào volume `pki` chỉ do provisioner ghi.

| Mục đích | CA | Certificate | Yêu cầu |
|---|---|---|---|
| WSS/HTTPS công khai | CA-public | gateway, control, identity (nếu bật HTTPS) | DNS SAN theo hostname §3.2 |
| RPC workload | CA-workload | actor, gateway | **đúng một** URI SAN `spiffe://signaling/local-test-only/cell/<cell>/(actor\|gateway)[/<id>]`, DNS SAN khớp `tlsAuthority`, TLS 1.3, EKU server+client |
| Remoting + management | CA-remoting-`<cell>` (một CA mỗi cell) | mỗi actor, PKCS12 keystore + truststore | IP SAN theo địa chỉ tĩnh, mutual auth |
| Database | CA-db | PG, client | `verify-full` |
| Source | CA-source | local-platform | DNS/IP SAN; caller đã enroll |
| Fake kube-API | CA-kube | kube-api | IP SAN; kèm file token bearer |

Khóa Ed25519: proof signing mỗi cell, ký source. Mật khẩu PKCS12 sinh ngẫu nhiên, nằm trong file mount; không vào image, không vào git, không vào log.

### 4.7 Formation và kube-API

- Lần dựng đầu dùng profile `initial-six`: `new-cluster-enabled=on` cùng `signaling.controlled-initial-formation=true`, sáu contact point; các lần khởi động lại tiếp tục dùng cùng cấu hình (Pekko bootstrap gia nhập cluster đã có).
- `kube-api` trả `GET /api/v1/namespaces/<ns>/pods?labelSelector=app=webrtc-signaling,plane=actor,cell=<cell>` với bearer token, JSON `items[]` gồm `status.podIP`, `phase=Running`, `containerStatuses`, `spec.containers[].ports[name=management]`. Danh sách lấy từ `topology.env`; mẫu hành vi: `NativeActorProcessIT`. Namespace giả `wsa-<cell>`.
- Đồng hồ khởi động local của actor là 30s kể từ lúc process bắt đầu: mười hai actor phải khởi động đồng thời, không nối tiếp.

## 5. Bố cục `deploy/compose/`

```
deploy/compose/
  compose.yaml            # mạng, volume, service, depends_on, healthcheck
  topology.env            # địa chỉ, cổng, tên cell: nguồn duy nhất
  .env.example            # phiên bản image pin; không có secret
  provisioner/            # Dockerfile (JDK + openssl + psql + jq), provision.sh, templates/
  kube-api/               # Dockerfile + server nhỏ, pin digest
  smoke/                  # package.json (ws pin), smoke.mjs
  README.md               # chạy, dừng, xóa, hosts, giới hạn, trạng thái NOT_QUALIFIED
  hosts.sh                # chỉ in dòng /etc/hosts cần thêm
```

Image: dùng `Dockerfile` hiện có cho `signaling-app`; thêm Dockerfile cho `signaling-identity` và `signaling-local-platform`. Mỗi service chạy UID 10001, `read_only: true`, `tmpfs: /tmp` (128 MiB), `cap_drop: [ALL]`, `security_opt: [no-new-privileges]`. JVM option local theo `values.schema.json` (`-XX:+ExitOnOutOfMemoryError`; kích thước heap chọn trong plan theo đo thật, mặc định đề xuất 512 MB). Tag image bằng digest, không dùng `latest`.

## 6. Trình tự khởi động

1. `provisioner` giai đoạn 1: CA, certificate, PKCS12, khóa Ed25519, mật khẩu, UUID `podUid`.
2. Các PostgreSQL khởi động; standby sao lưu base từ primary.
3. `provisioner` giai đoạn 2: Flyway cell và directory/source, seed, role replication, chờ `sync`, đọc `systemIdentifier`/timeline, render `provider.json`, `runtime.yaml`, `directorySnapshot` + SHA-256 cho từng container, ghi vào `/run/signaling/<service>/`.
4. `kube-api`, `local-platform`, `identity`.
5. Mười hai actor đồng thời, sau đó `gateway-*` và `control`.

Tất cả dùng `depends_on` với `condition: service_healthy` hoặc `service_completed_successfully`. Healthcheck của JVM: `bash -c` mở `/dev/tcp/<ip>/8559` và kiểm dòng trạng thái `200` của `GET /ready` (image JRE không có `curl`).

## 7. Smoke test

`deploy/compose/smoke/smoke.mjs` (Node ≥ 20, `ws` pin phiên bản), thoát khác 0 khi sai:

1. `POST /api/v1/auth/login` cho `admin` và `mod` (tài khoản của module identity) → JWT.
2. `POST /v1/signaling/bootstrap` cho từng user; xác nhận hai user rơi vào hai cell khác nhau (nếu cần, chọn userId để bảo đảm điều này).
3. Mở WSS tới URL trả về, `Origin` nằm trong allowlist, subprotocol `webrtc-signaling.v1`, gửi `AUTH`, kỳ vọng `AUTH_OK`.
4. `INVITE` cross-cell → `ACCEPT` → `OFFER`/`ANSWER`/`ICE_CANDIDATES` → `HANGUP`, kiểm `ACK_COMMITTED`, `callVersion` đơn điệu và sự kiện ở phía kia.
5. Thêm revoke qua endpoint admin của source → socket đóng với lý do bảo mật.

Cấu trúc payload chính xác lấy từ `ProtocolValidator`, kiểm trong plan.

## 8. Rủi ro đã biết và cách xử lý

| Rủi ro | Xử lý |
|---|---|
| `NativeCompatibility` đọc `...artery.ssl.config.require-mutual-authentication` nhưng conf đặt dưới `ssl.config-ssl-engine`; gate tương thích có thể không bao giờ mở | SP1 xác minh trên process thật; sửa nơi đọc hoặc đặt khóa tương ứng trong Config được inject, kèm test |
| Epoch phát hành của Identity so với watermark `RevocationState` (nguy cơ epoch 0 bị coi là đã revoke) | Kiểm bằng test trước khi bật smoke bước 5; không đổi ngữ nghĩa `RevocationState` |
| Control cần `CellRpcClient`; `NativeCellRpcConfiguration` hiện chỉ profile `actor`, `gateway` | SP1 cấp enrollment tương thích riêng cho control hoặc mở rộng cấu hình có test |
| Cổng thời gian 30s của actor khi khởi động | Khởi động đồng thời; nếu compose chậm, tăng giới hạn trong chính config có kiểm range, không bỏ kiểm tra |
| Phép đo đồng hồ chỉ trong container | Ghi rõ trong evidence; không dùng làm bằng chứng kỷ luật đồng hồ host |
| Control giữ credential hai cell | Chỉ `LOCAL_TEST_ONLY`; đóng bằng A3/A4 |
| Jar `signaling-app` trong `target/` có thể cũ | Makefile/README yêu cầu `./mvnw -pl signaling-app,signaling-identity,signaling-local-platform -am package` trước khi dựng image |
| Tài nguyên (~10 GB RAM, tải CPU cao làm flake deadline DB 2s) | README ghi yêu cầu tối thiểu; healthcheck có thời gian chờ đủ; không hạ deadline production |

## 9. Tiêu chí chấp nhận

1. Từ checkout sạch, `docker compose up` đưa mọi service đến `healthy`; mười hai actor `Up`; `/ready` của gateway, control và actor trả 200.
2. `smoke.mjs` PASS, gồm cross-cell và revoke.
3. Dừng một actor: cell vẫn `ready` (quorum 4) và hồi phục khi khởi động lại. Dừng standby của một cell: lệnh critical của cell đó fail-closed, không có fallback bất đồng bộ.
4. Fingerprint lệch, certificate sai SPIFFE hoặc thiếu contract: container từ chối khởi động với lỗi rõ.
5. `./mvnw verify` của SP1/SP2 xanh; image triển khai không chứa `TestOnlyNativeCapabilities`.
6. Không có secret hay private key mới trong git; mọi artifact và log in `LOCAL_TEST_ONLY` và `NOT_QUALIFIED`.

Đạt 1–6 chỉ cho phép trạng thái `LOCAL_QUALIFIED`. Production, HA thật, physical fencing, N−1 và 10M vẫn `NOT_QUALIFIED`.
