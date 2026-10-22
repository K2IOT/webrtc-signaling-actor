# Docker Compose hai cell — môi trường signaling chạy được trên một máy

Date: 2026-10-21
Status: DRAFT — SP1 (Local runtime adapter) và SP2 (Local platform, `signaling-local-platform`) đã triển khai trên `dev-docker-fix`, cùng image contract và tài liệu `config/native-sources.md`; spec bằng văn bản chờ rà soát (thiết kế đã được người dùng duyệt ở mức hội thoại, 2026-10-21). SP3 (`deploy/compose/`) đã triển khai tới Task 9 (provisioner, PostgreSQL, kube-api, compose, smoke, Makefile, preflight, README); nghiệm thu §9 (Task 10) **NOT_RUN** nên chưa đạt `LOCAL_QUALIFIED`. Các mục §3, §4.1–§4.6, §5, §8 phản ánh những gì SP1, SP2 và SP3 thực sự xây (as-built); các chỗ lệch so với bản thiết kế đầu được sửa tại chỗ. Mọi thứ vẫn `LOCAL_TEST_ONLY` và `NOT_QUALIFIED`.
Environment: **LOCAL_TEST_ONLY** / `local-minikube` / `local-test-only`.
Qualification: **NOT_QUALIFIED**. Compose chỉ có thể đạt `LOCAL_QUALIFIED`. Không có tuyên bố production, HA thật, physical fencing hay 10M.
Source baseline: branch `dev-docker-fix`.

## 1. Mục tiêu và phạm vi

Chạy được luồng `Login → JWT RS256 → Bootstrap → Directory resolve → WSS → Gateway → Actor RPC → Pekko actor → PostgreSQL → cross-cell signaling` trên một máy bằng `docker compose up`, với hai home cell `c001`, `c002`, mỗi cell có Pekko cluster, cell PostgreSQL và gateway riêng, cùng một control, Directory PostgreSQL, Identity và source service dùng chung. SDP/ICE chỉ là signaling metadata; media không đi qua hệ thống.

Hiện trạng làm cho mục tiêu này không đạt được chỉ bằng file compose (đã xác minh bằng đọc source ngày 2026-10-21):

- Mỗi plane chỉ lắp ráp khi có bean `Native*Enrollment`; trong `src/main` chỉ `NativeCompatibilityCollectors` tạo một enrollment, phần còn lại do test đăng ký. Image hiện từ chối khởi động với `NATIVE_RUNTIME_NOT_INSTALLED` (`qualification/scenarios/image-contract.sh`).
- `MountedNativeContract` chỉ parse và validate, không tạo bean.
- Không có source service cho đồng hồ và revocation đã ký, không có bộ sinh PKI, không có công cụ migrate/seed, và Pekko discovery bắt buộc `kubernetes-api`.

Hai gạch đầu là hiện trạng trước SP1. Sau SP1, `LocalRuntimeInitializer` (đăng ký trong `spring.factories`) dựng các enrollment từ contract mount cho cả ba plane (§4.3); gạch thứ ba vẫn đúng và thuộc SP2/SP3.

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

### 3.1 Service (26 container: 17 JVM, 5 PostgreSQL, 2 kube-api, 2 bước provisioner)

| Nhóm | Container | Ghi chú |
|---|---|---|
| Mỗi cell (×2) | `actor-<cell>-1..6` | Cố định 6 (`SignalingProperties.Cluster` yêu cầu), quorum 4, `required-contact-point-nr=6` |
| | `gateway-<cell>` | WSS 8443, relay gRPC 9443 |
| | `pg-<cell>-primary`, `pg-<cell>-standby` | PostgreSQL 17.6 |
| | `kube-api-<cell>` | Fake Kubernetes API cho Pekko, một cái cho mỗi cell, nằm trên mạng cell của nó |
| Dùng chung | `control` | HTTPS 8443, giữ credential cả hai cell DB |
| | `identity` | HTTP 8081 |
| | `local-platform` | Source service, HTTPS/mTLS 9443 (source), 9444 (admin) |
| | `pg-directory` | Hai database: `directory`, `source` |
| | `provision-1`, `provision-2` | Hai bước one-shot dùng chung một image (§6) |

17 JVM = 15 JVM plane (12 actor, 2 gateway, `control`) + `identity` + `local-platform`. 5 PostgreSQL = 4 cell + `pg-directory`. Bản thiết kế đầu ghi 14 JVM và một `kube-api` dùng chung; kube-api tách theo cell để mỗi cell chỉ thấy kube-API của chính nó (§3.2).

Ước tính tài nguyên: 17 JVM × heap tối đa 384 MB (`-Xmx384m`, mặc định của `.env.example`) cộng overhead JVM, và 5 PostgreSQL; sàn preflight là 12 GiB RAM trống. Đây là ước lượng, không phải đo; Task 10 đo (`docker stats`) và chỉnh `JVM_OPTS` cùng sàn preflight.

### 3.2 Mạng và địa chỉ tĩnh

Discovery dùng `use-raw-ip=true` và management client kiểm hostname bằng thuật toán `HTTPS`, nên mọi actor phải có IP cố định và certificate có IP SAN.

| Mạng | Subnet | Chứa | Dùng cho |
|---|---|---|---|
| `net-c001` | 172.30.1.0/24 | actor c001 (.11–.16), `control` (.10), `pg-c001-primary` (.101), `pg-c001-standby` (.102), `kube-api-c001` (.250) | Artery 25520, management 8558, DB, discovery |
| `net-c002` | 172.30.2.0/24 | tương tự (.11–.16, .10, .101, .102), `kube-api-c002` (.250) | như trên |
| `net-shared` | 172.30.0.0/24 | actor c001 (.31–.36), actor c002 (.41–.46), `gateway-c001` (.21), `gateway-c002` (.22), `control` (.10), `identity` (.11), `local-platform` (.12), `pg-directory` (.14) | RPC 8443/9443, bootstrap, source |

Artery và management chỉ lên `net-<cell>`, nên c001 không có đường tới Artery của c002. `control` nối cả `net-c001`, `net-c002` và `net-shared` để tới ba PostgreSQL; địa chỉ trên hai mạng cell là `CONTROL_C001_IP`=172.30.1.10 và `CONTROL_C002_IP`=172.30.2.10 (bảng địa chỉ đầu tiên không liệt kê, dùng host .10 còn trống). Có hai `kube-api`, mỗi cái chỉ nằm trên mạng cell của nó (`KUBE_C001_IP`, `KUBE_C002_IP`). Hai mạng cell là `internal: true` (không có egress); `net-shared` là bridge thường vì các cổng publish cần nó. Mỗi mạng khai `ip_range` (`NET_*_DYNAMIC`, `.192/27`) cho địa chỉ động của `provision-2` (nối cả ba mạng); mọi địa chỉ tĩnh nằm ngoài các dải này nên container một lần không bao giờ chiếm địa chỉ mà service sau cần (`pg-directory` nhận `control` và `local-platform` theo /32). Địa chỉ cụ thể nằm trong `topology.env` và là nguồn duy nhất cho compose, generator PKI và contract.

Cổng publish ra host: gateway c001 `8443`, gateway c002 `8444`, control `8445`, identity `8081`. Hostname `c001.signaling.local`, `c002.signaling.local`, `bootstrap.signaling.local`, `identity.signaling.local` trỏ về `127.0.0.1` qua `/etc/hosts` (script in dòng cần thêm, không tự sửa file hệ thống).

## 4. Hợp đồng giữa SP1, SP2, SP3

### 4.1 Contract mount (SP3 sinh, SP1 đọc)

Dùng `MountedNativeContract` schema v1 (`qualification=LOCAL_TEST_ONLY`, `environment=local-test-only`), nạp bằng `load(file, "local-minikube", "LOCAL_TEST_ONLY")`. Mỗi container có thư mục `/run/signaling/` (read-only, mode 0440, group 10001) chứa `provider.json`, `runtime.yaml` và mọi file mà contract tham chiếu. Ràng buộc đã có trong parser và phải được generator tôn trọng:

- File ≤ 64 KiB; mọi đường dẫn tuyệt đối, dưới thư mục chứa contract, ≤ 512 ký tự.
- JDBC URL chứa `sslmode=verify-full`.
- `cells[]` chứa cell của chính nó với cùng `storageEpoch`; `gateways[]` không rỗng; `proofs.publicKeys` chứa khóa `"<cell>/<keyId>"` của chính nó.
- `directorySnapshotSha256` là SHA-256 hex của `directorySnapshot`.
- `identity.issuer/audience` khớp `SIGNALING_IDENTITY_ISSUER/AUDIENCE`; `maximumJwtLifetime` ≥ 15 phút; `revocationPropagationSLO < hardSafetyBound < maximumJwtLifetime`; `clockSkew` ≤ 30s; `preserveJtiOnRefresh=true`.

Ngoài `provider.json`, mỗi container có `process.json` trong cùng thư mục: hồ sơ process theo container (plane, `podUid`, `podIp`, các cổng, `azRole` và `cellAzRoles`, `gatewayId`/`gatewayBootId`, `wal`, `compatibility`, `kube`, `transferCells`, `privacyPseudonymKey`, `allowedOrigins`). Vị trí lấy từ `SIGNALING_LOCAL_PROCESS_PROFILE` (mặc định `/run/signaling/process.json`); vị trí contract từ `SIGNALING_NATIVE_PROVIDER_CONTRACT` (mặc định `/run/signaling/provider.json`). Schema, quy tắc validate và dạng lỗi nằm ở `config/native-sources.md`, mục "Local process profile"; renderer của SP3 phải sinh đủ mọi khóa (giá trị tùy chọn là `null` tường minh, khóa thiếu bị từ chối). Ràng buộc thêm so với contract:

- `process.json` phải nằm cùng thư mục thật với `provider.json` (nếu không: `profile/PATH_ESCAPE`); mọi đường dẫn trong cả hai file, kể cả `kube.caPath` và `kube.tokenPath`, được realpath và phải nằm dưới thư mục đó.
- **Quy tắc kích hoạt**: cần cả `SIGNALING_DEPLOYMENT_MODE=local-minikube`, `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY` **và** một vị trí contract tường minh (`SIGNALING_NATIVE_PROVIDER_CONTRACT` hoặc `SIGNALING_LOCAL_PROCESS_PROFILE` không rỗng; renderer compose của SP3 sẽ luôn đặt biến thứ nhất). Chỉ có dấu hiệu mà thiếu vị trí thì adapter không đăng ký gì và khởi động vẫn thất bại với `NATIVE_RUNTIME_NOT_INSTALLED`. Khi cổng đã mở, file thiếu hoặc hỏng dừng khởi động bằng đúng một dòng cố định `LOCAL_RUNTIME_REJECTED` kèm vai trò và loại lỗi (không có đường dẫn, giá trị hay nguyên nhân). Contract đọc được nhưng khai báo môi trường khác `local-test-only` thì adapter không đăng ký gì.
- **Phạm vi SP1: adapter chỉ dành cho compose.** Hiện chưa có gì sinh `process.json`; SP3 (renderer compose) sở hữu việc sinh profile và contract mount. Chart Helm luôn đặt `SIGNALING_NATIVE_PROVIDER_CONTRACT`, nên một release Helm `local-minikube` giờ mở cổng kích hoạt và đóng khi lỗi với `LOCAL_RUNTIME_REJECTED` `profile/MISSING` (trước SP1 là `NATIVE_RUNTIME_NOT_INSTALLED`). Release đó cũng không bao giờ khởi động được: `SIGNALING_POD_UID` của chart lấy từ `metadata.uid` của pod nên không thể bằng `podUid` của profile. Helm không phải consumer được hỗ trợ của adapter; chart và các kiểm tra render của nó không đổi.
- `cellAzRoles` (actor): 2–3 vai trò `az-*` khác nhau, gồm `azRole` của chính actor, giống nhau trong cả cell; để rỗng ở gateway và control.
- `compatibility`: `major` ≥ 1, `executableId` là 64 hex chữ thường, `features` chỉ gồm tên đã biết, `upperMinor = lowerMinor + 1`, `minor` thuộc cửa sổ.
- `transferCells`: chỉ control, **đúng một** phần tử (cell còn lại).
- `privacyPseudonymKey`: file khóa thô 32–64 byte (actor).

`runtime.yaml` chỉ chứa các section `signaling` có kiểu (`identity`, `lease`, `cluster`, `protocol`, `transport`, `queues`, `calls`, `rate-limits`), một root, scalar-only.

### 4.2 Biến môi trường mỗi container

Mọi plane: `SPRING_PROFILES_ACTIVE` (đúng một trong `gateway|actor|control`, phải khớp `plane` trong `process.json`), `SIGNALING_DEPLOYMENT_MODE=local-minikube`, `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY`, `SIGNALING_NATIVE_PROVIDER_CONTRACT=/run/signaling/provider.json`, `SIGNALING_LOCAL_PROCESS_PROFILE=/run/signaling/process.json`, `SIGNALING_CELL_ID` (bằng `cell` của contract), `SIGNALING_POD_UID` (bằng `podUid` của profile), `SIGNALING_CLUSTER_FINGERPRINT` (64 hex chữ thường, **một giá trị duy nhất cho cả hai cell** — `CLUSTER_FINGERPRINT` trong `topology.env`; "giống nhau trong cell" chỉ là cận dưới và không đủ vì các cổng tương thích (`CompatibilityWindow.accepts`, `NativeCompatibility.permitsGateway`, `NativeCompatibilityCollectors.validate`) so fingerprint của peer với fingerprint của chính build, nên giá trị theo cell làm mọi INVITE và relay chéo cell đóng khi lỗi), `SIGNALING_IDENTITY_ISSUER` và `SIGNALING_IDENTITY_AUDIENCE` (bằng `identity` của contract), `SIGNALING_RUNTIME_CONTRACT=/run/signaling/runtime.yaml`. Lệch `SIGNALING_POD_UID`, `SIGNALING_CELL_ID`, issuer hoặc audience cho `identity/IDENTITY_MISMATCH`.

Riêng actor:

- `SIGNALING_KUBE_API_HOST` và `SIGNALING_KUBE_API_PORT` phải là biến môi trường **thật** của process (không phải system property hay application property), bằng đúng `kube.apiHost` và `kube.apiPort` của profile; Pekko discovery đọc địa chỉ từ môi trường process nên chỉ nguồn này thỏa kiểm tra (lệch cho `profile/IDENTITY_MISMATCH`).
- `SIGNALING_FORMATION_PROFILE=initial-six` ở lần dựng đầu (§4.7); giá trị khác hoặc vắng thì chỉ gia nhập cluster đã có. SP1 đã tiêu thụ biến này.
- Ba biến mật khẩu `SIGNALING_REMOTING_KEYSTORE_PASSWORD`, `SIGNALING_REMOTING_KEY_PASSWORD`, `SIGNALING_REMOTING_TRUSTSTORE_PASSWORD` lấy từ file mount, không đặt trong file compose.
- `SIGNALING_POD_IP`, `SIGNALING_NAMESPACE`, `SIGNALING_ACTOR_SERVICE`, `SIGNALING_DISCOVERY_SELECTOR`, `SIGNALING_REMOTING_KEYSTORE`, `SIGNALING_REMOTING_TRUSTSTORE` không còn bắt buộc: adapter đè các giá trị đó từ `podIp`, `kube.namespace`, tên cell và đường dẫn store trong contract.

`SIGNALING_POD_UID` là UUID cố định mỗi container, sinh bởi provisioner. `processBoot` luôn được sinh mới trong JVM và đăng ký với source service trước khi lấy báo cáo đồng hồ.

### 4.3 Nhiệm vụ SP1 — Local runtime adapter

Trạng thái: **đã triển khai** (`signaling-app/.../runtime/local`, branch `dev-docker-fix`). Kích hoạt theo quy tắc tường minh ở §4.1 (hai dấu hiệu cùng vị trí contract; contract khai báo môi trường khác thì không đăng ký gì); image production vẫn từ chối contract local. Kết quả theo từng mục của nhiệm vụ:

1. **Enrollment từng plane** (danh sách ở `docs/superpowers/plans/2026-10-13-minikube-authority-runtime.md` A6): gateway business/ingress/lifecycle/cell-RPC/compatibility; actor process/source/business/ingress/scheduling/maintenance/WAL/cell-RPC/compatibility; control database/source/business/ingress/bucket-transfer/cell-RPC. Mọi bean đăng ký bằng `registerBean(Class, Supplier)`, không dùng `registerSingleton`, để `@ConditionalOnBean` thấy định nghĩa.
2. **`SqlTransactions`, `DbBoundary`, `DbPools` của actor** dựng từ `cellDatabase`; actor không bao giờ mở `directoryDatabase`.
3. **`BoundedTokenVerifier`** dựng từ `jwtPublicKeys` (RS256, modulus RSA ≥ 2048 bit) cho cả ba plane; khóa hỏng dừng khởi động trước khi có bean nào.
4. **Pekko `Config` của actor**: `application-actor.conf` là nền; chỉ giá trị do contract, profile và môi trường quyết định được phủ lên. Vai trò vùng lấy từ `azRole` và `cellAzRoles` (adapter chấp nhận 2–3 vùng khác nhau mỗi cell; SP3 dùng `az-a`/`az-b`, 3+3 actor mỗi cell); khóa `signaling.compatibility.*` và cửa sổ; override discovery (`api-ca-path`, `api-token-path`, tên biến môi trường host/port); keystore/truststore PKCS12; Artery và management chỉ bind `podIp`.
5. **`ClusterReadiness.localMinikube("LOCAL_TEST_ONLY")`** thay bean mặc định.
6. **Nguồn đồng hồ/revocation phía client**: actor qua `NativeActorSourceEnrollment`; gateway qua `NativeClockSource` và `NativeCachedRevocationSource` kèm worker; control qua `NativeControlSourceEnrollment` và `NativeCachedRevocationSource`. Gateway chỉ nạp khóa công khai để kiểm proof relay, không nạp khóa ký proof của cell.
7. **Tương thích của control**: control mang một danh tính workload riêng `control` (`spiffe://signaling/local-test-only/cell/<cell>/control`) và quan sát tương thích của actor bằng `CellRpcClient`. Actor chỉ nhận danh tính này cho thao tác quan sát tương thích, từ chối cho execute, relay và sync. Không mượn danh tính actor hay gateway, không dùng hằng số cho cổng. Cổng `controlReady` mở từ quan sát thật.
8. **`NativeBucketTransferEnrollment`** cần hai store: store của cell local của control và store của **đúng một** `transferCells` (cell kia). Không phần tử hoặc nhiều hơn một cho `profile/INVALID` vì enrollment chỉ nhận một nguồn và một đích.

Các thành phần chưa có bản production nên SP1 dùng thế thân **từ chối mọi yêu cầu** (đóng khi lỗi, không giả thành công):

- Privacy detachment từ chối mọi detachment: yêu cầu xóa dữ liệu ở lại trạng thái chờ, không bao giờ báo hoàn tất.
- Epoch adoption bị từ chối.
- Admission ghi vào directory của control là hằng `true` (có chú thích): compose chỉ chạy đúng một control; hàng rào thật chờ A3/A4.

Với compose điều này có nghĩa: **không có xóa dữ liệu riêng tư**, **không có epoch adoption, promotion hay migration**; kịch bản nào cần chúng không chạy được cục bộ.

Không đưa `TestOnlyNativeCapabilities`, đồng hồ giả, WAL fact giả hay readiness vô điều kiện vào image triển khai. Lưu ý đồng hồ do source của test tự khai (SP2 phải đo thật, §4.4).

### 4.4 Nhiệm vụ SP2 — `signaling-local-platform`

**Đã triển khai (SP2)**, `LOCAL_TEST_ONLY` và `NOT_QUALIFIED`. Hợp đồng chi tiết (cổng, khóa config, quy tắc danh tính, endpoint và mã trạng thái, ngữ nghĩa đồng hồ và epoch, giới hạn đã biết) nằm ở `config/native-sources.md`, mục "Local platform source (LOCAL_TEST_ONLY)"; image: `Dockerfile.local-platform`, kiểm bằng `image-contract.sh`. Bằng chứng: `LocalPlatformCompositionIT` (`signaling-integration-tests`; một cell thật chạy với source của SP2, gồm khởi động chỉ sau khi đồng hồ đo được ấm lên, revoke qua endpoint admin làm các plane từ chối user, và bước lùi đồng hồ làm các plane không an toàn trở lại), `SourceServerIT` (mTLS, danh tính, mã trạng thái, framing, cô lập vai trò admin), `JdbcRevocationLogIT` (log append-only, offset không hở, thứ tự commit), `ClockReportServiceTest` và `HostClockObserverTest` (số liệu đo và trạng thái không an toàn). Không có gì ở đây là bằng chứng về kỷ luật đồng hồ của host.

Server HTTPS/mTLS cổng 9443, TLS 1.3, chỉ nhận caller đã enroll; tuân theo wire contract trong `config/native-sources.md`: header `X-Signaling-Pod-Uid`, `X-Signaling-Process-Boot`, `X-Signaling-Source-Offset`; `Content-Length` đúng, `Connection: close`, không redirect/chunked/nén, body ≤ 32768 byte.

- **Đồng hồ**: báo cáo ký Ed25519 trên `ClockSafetyMonitor.signingBytes`, hiệu lực ≤ 5s, `pairUncertainty` ≤ 250000µs, `relativeRateError` ≤ 1000 ppm. Số liệu đến từ đo thật (lệch giữa wall clock và monotonic clock qua các mẫu liên tiếp); không có đo thì trả trạng thái không an toàn, không bao giờ hằng số an toàn. Giới hạn của phép đo (chỉ thấy đồng hồ của container, không phải kỷ luật thời gian của host) được in vào evidence. Khi triển khai: hiệu lực là 2s, và `pairUncertainty` báo cáo = giá trị đo + phụ cấp cố định 100000µs (`CONSUMER_OBSERVATION_ALLOWANCE_MICROS`) cho độ trễ đọc đồng hồ của chính consumer, thứ source không đo được; `continuous` là false cho đến khi có ≥ 2s mẫu sạch và trong 5s sau một bước nhảy hay đứng đồng hồ.
- **Revocation**: log append-only bền trong database `source` của `pg-directory`, offset và high-water đơn điệu, mỗi trang ≤ 16 mục (sự kiện + khóa bị loại), ký bằng `RevocationSourceVerifier.signingBytes`. Endpoint admin (mTLS, vai trò riêng) để thêm revoke user/token.
- `registerBoot(podUid, processBoot, cell, plane)` gắn với danh tính certificate; boot mới thay boot cũ của Pod đó; header không tự đăng ký được danh tính.
- Chạy được độc lập, yêu cầu `LOCAL_TEST_ONLY` trước khi khởi động; không nằm trong auto-configuration của `signaling-app`.

### 4.5 Cơ sở dữ liệu (SP3 sinh, SP1 kiểm)

- **Cell DB** (`signaling`): Flyway `signaling-storage/src/main/resources/db/migration/V001–V018` với bảng lịch sử mặc định `flyway_schema_history` (cổng tương thích so checksum V016 với CRC32 của file). App không chạy Flyway (bị loại khỏi auto-config).
- **Directory DB** (`directory`): `signaling-control-api/src/main/resources/db/directory/migration/V001–V002` với bảng lịch sử riêng. **Source DB** (`source`): migration của `signaling-local-platform`, tách khỏi mọi bảng cell.
- **Seed cell**: `cell_authority(1,'<cell>',1,'GROUPED',1,'ACTIVE')`; `bucket_authority` đủ 16384 hàng cho bucket 0–16383 (`directory_epoch=1`, `recovery_epoch=1`): bucket được gán cho cell đó có `status='ACTIVE'`, các bucket còn lại `status='FROZEN'` (không phục vụ nhưng có hàng sẵn cho transfer); phép chia bucket giữa hai cell phải trùng với seed directory; `group_owner` cho nhóm 0–1023 (`ownership_hash_version=1`, `storage_epoch=1`, `group_epoch=1`, `lease_sequence=0`, `IDLE`).
- **Seed directory**: 16384 dòng `directory_bucket`, chia đôi giữa `c001` và `c002`, `wss_url` trỏ `wss://c001.signaling.local:8443/ws` và `wss://c002.signaling.local:8444/ws`.
- **`directoryDatabase` bắt buộc ở mọi plane** (contract). Actor và gateway mang credential `signaling_nodir`, role không có `CONNECT` trên database `directory` nên không đọc được gì; `control` mang `signaling_app`. `pg_hba` của `pg-directory` cho `signaling_app` vào `directory` chỉ từ `CONTROL_IP/32` và `source_owner` vào `source` chỉ từ `PLATFORM_IP/32`; `signaling_nodir` từ subnet `net-shared`. Role: `signaling_owner` (superuser của cụm, chỉ provisioner dùng), `signaling_app`, `signaling_nodir`, `replicator`, `source_owner` (chủ database `source`, chỉ `local-platform` dùng và tự chạy Flyway của nó). Mật khẩu sinh một lần ở giai đoạn 1.
- **Directory snapshot** (`directorySnapshot`, SP3 render, SP1 đọc): JSON ≤ 64 KiB, SHA-256 hex của đúng các byte đó phải bằng `directorySnapshotSha256`, parser nghiêm ngặt (khóa lạ/trùng/thiếu, nội dung thừa, số thập phân bị từ chối). `buckets` gồm **tối đa 512 dải** không chồng lấn trên bucket 0–16383; mỗi dải gán `(cell, directoryEpoch, storageEpoch)` với `cell` thuộc `cells[]` của contract và đúng `storageEpoch` của cell đó; bucket không dải nào phủ thì không có home tin cậy và mọi kiểm tra home của user đó thất bại. Phép chia phải trùng seed `directory_bucket` (actor không đọc bảng này nên renderer chịu trách nhiệm). `gatewayBoots` (tối đa 128) có **đúng một** mục `(cell, gatewayId, bootId)` cho mỗi `gateways[]` của contract, và `bootId` phải bằng `gatewayBootId` trong `process.json` của gateway đó (gateway lệch thì dừng khởi động với `identity/IDENTITY_MISMATCH`). Mọi actor đọc cùng một snapshot, không nhân bản `gatewayBoots` vào sáu profile.
- **`bootId` của gateway được ghim cho mỗi lần provision**: enrollment tương thích cần biết trước bootId của mọi gateway, trong khi một gateway khởi động lại không được dùng lại bootId cũ (lease boot đã hết hạn không gia hạn hay tạo lại được). Vì vậy **khởi động lại riêng container gateway không được hỗ trợ**. Cách phục hồi được hỗ trợ là `make reset` (`docker compose down -v`) rồi `make up`: provision lại từ volume trống. Lý do không đủ với `compose down` rồi `up` thường: các bước provisioner chạy với `--if-needed` (compose khởi động lại container one-shot đã thoát ở mỗi lần `up`), nên giai đoạn đã xong bị bỏ qua và gateway khởi động lại dùng đúng bootId đã hết lease, không bao giờ sẵn sàng. Vòng đời được hỗ trợ là `make up` → dùng → `make down` (luôn `-v`); `make up` từ chối (`guard.sh`) khi volume `run` còn mà một service JVM không chạy, hoặc volume có dấu `.offline` của một lần render offline. Hệ quả: không khởi động ấm lại được một stack đã dừng. README nêu giới hạn này.
- **WAL**: role replication, slot hoặc `application_name` cố định cho standby; provisioner chờ standby `sync` trong `pg_stat_replication`, đọc `systemIdentifier` và timeline, ghi vào `NativeWalEnrollment` (không dùng `localSingleWriter`). Cần quyền đọc `pg_stat_replication`, `pg_control_system()` và `pg_current_wal_flush_lsn()`; provisioner cấp bằng `pg_monitor` (quyền chính xác xác minh trong plan).
- PostgreSQL ≥ 17 vì `SET LOCAL transaction_timeout`. Image `postgres:17.6` đúng như pin trong `config/compatibility-manifest.yaml`.

### 4.6 PKI

Generator thuần `openssl` + `keytool` (`provisioner/lib/pki.sh`), đọc địa chỉ và tên từ `topology.env`, ghi vào volume `run` chỉ do provisioner ghi; mỗi service mount read-only đúng thư mục con của mình. Khóa và mật khẩu sinh ngẫu nhiên ở mỗi lần provision (không deterministic).

| Mục đích | CA | Certificate | Yêu cầu |
|---|---|---|---|
| HTTPS công khai | CA-public | identity (nếu bật HTTPS) | DNS SAN theo hostname §3.2 |
| RPC workload | CA-workload | actor, gateway | **đúng một** URI SAN `spiffe://signaling/local-test-only/cell/<cell>/(actor\|gateway)[/<id>]`; mỗi actor mang id riêng của pod (`…/actor/actor-c001-3`) để source phân biệt các pod (`config/native-sources.md`, "Known limits"), DNS SAN khớp `tlsAuthority`, TLS 1.3, EKU server+client; gateway thêm DNS SAN công khai cho WSS (§3.2) |
| Control (HTTPS công khai + RPC quan sát) | CA-workload (CA mà actor tin) | control | **đúng một** URI SAN `spiffe://signaling/local-test-only/cell/<cell>/control`, cộng DNS SAN công khai cho HTTPS (§3.2), TLS 1.3, EKU server+client |
| Remoting + management | CA-remoting-`<cell>` (một CA mỗi cell) | mỗi actor, PKCS12 keystore + truststore | IP SAN theo địa chỉ tĩnh, mutual auth |
| Database | CA-db | PG, client | `verify-full` |
| Source và admin | CA-workload (không có CA-source riêng) | local-platform | DNS SAN `local-platform.signaling.local` và IP SAN; một keystore `platform.p12` cho cả hai listener (source 9443, admin 9444); caller source đã enroll |
| Admin client | CA-admin | client admin của smoke | **đúng một** URI SAN `…/admin/smoke`; `local-platform` chỉ tin CA-admin ở listener admin; `admin-client.crt/.key` và CA nằm trong `_shared/` của volume `run`, `make smoke-ca` xuất chúng cho client smoke |
| Fake kube-API | CA-kube (một CA, leaf IP SAN riêng cho mỗi cell) | `kube-api-<cell>` | IP SAN; token bearer riêng cho mỗi cell, file mount cùng actor của cell |

Gateway và control chỉ nạp một leaf (`tls.certificate`) cho cả listener công khai lẫn RPC. Leaf của gateway vì vậy mang cả DNS SAN công khai lẫn URI SAN workload và chain tới CA-workload (client smoke tin CA đó); hàng "HTTPS công khai" chỉ còn áp cho identity.

Danh tính `control` **không gắn với một cell khi quan sát tương thích**, theo thiết kế: server của actor chỉ kiểm vai trò, như với gateway, và control của một cell quan sát được actor của cell khác. Control poll actor qua làn admission `CONTROL` của actor (tính vào sức chứa của làn này). Danh tính này bị từ chối cho execute, relay và sync.

Khóa Ed25519: proof signing mỗi cell, ký source. Mật khẩu PKCS12 sinh ngẫu nhiên, nằm trong file mount; không vào image, không vào git, không vào log.

### 4.7 Formation và kube-API

- Lần dựng đầu dùng profile `initial-six`: `new-cluster-enabled=on` cùng `signaling.controlled-initial-formation=true`, sáu contact point; các lần khởi động lại tiếp tục dùng cùng cấu hình (Pekko bootstrap gia nhập cluster đã có).
- `kube-api` trả `GET /api/v1/namespaces/<ns>/pods?labelSelector=app=webrtc-signaling,plane=actor,cell=<cell>` với bearer token, JSON `items[]` gồm `status.podIP`, `phase=Running`, `containerStatuses`, `spec.containers[].ports[name=management]`. Danh sách lấy từ `topology.env`; mẫu hành vi: `NativeActorProcessIT`. Namespace giả `wsa-<cell>`.
- Đồng hồ khởi động local của actor là 30s kể từ lúc process bắt đầu: mười hai actor phải khởi động đồng thời, không nối tiếp.

## 5. Bố cục `deploy/compose/`

```
deploy/compose/
  compose.yaml            # mạng, volume, service, depends_on, healthcheck
  topology.env            # địa chỉ, cổng, tên cell, id: nguồn duy nhất
  .env.example            # tag image và JVM_OPTS; không có secret (.env được gitignore)
  Makefile                # build, hosts, preflight, up, smoke-ca, smoke, down, reset, lint, test
  preflight.sh            # kiểm tra host trước make up
  guard.sh                # từ chối make up trên trạng thái sót lại (§4.5)
  status.sh               # bảng trạng thái và log của service lỗi cho make up
  hosts.sh                # chỉ in dòng /etc/hosts cần thêm
  README.md               # chạy, dừng, xóa, hosts, giới hạn, trạng thái NOT_QUALIFIED
  lib/topology.sh         # đọc topology.env, danh sách service, địa chỉ
  provisioner/            # Dockerfile, provision.sh (stage1/stage2), lib/ (pki, dbprep, db, render), sql/, templates/, tests/
  postgres/               # start-*.sh và pg_hba.conf.tmpl, được provisioner đưa vào mount PG
  kube-api/               # Dockerfile + server.mjs (Node, không phụ thuộc), pin digest
  smoke/                  # smoke.mjs, call-flow.mjs, payloads.mjs, package.json (ws pin)
  lint/                   # compose_lint.py: kiểm mô hình compose đã giải
  tests/                  # run-tests.sh và test_*.sh
```

Image: `Dockerfile` hiện có cho `signaling-app`, cộng `Dockerfile.identity`, `Dockerfile.local-platform`, `deploy/compose/kube-api/Dockerfile` và `deploy/compose/provisioner/Dockerfile`. Năm image của repo dùng tag `:local` (`signaling-app`, `signaling-identity`, `signaling-local-platform`, `signaling-kube-api`, `signaling-provisioner`), dựng từ checkout bằng `make build` và ghi commit vào label `org.opencontainers.image.revision` (`SOURCE_COMMIT`); tag cục bộ là mutable. Chỉ image bên thứ ba kéo về (PostgreSQL, base Node và JRE) được ghim digest; không dùng `latest`. Mỗi service JVM chạy UID 10001, `read_only: true`, `tmpfs: /tmp` 128 MiB `nosuid` — **không `noexec`**, vì thư viện native (netty epoll, lz4, lmdbjava, agrona) được giải nén vào `java.io.tmpdir` và nếu `noexec` sẽ lặng lẽ rơi về NIO, làm compose chạy một transport khác với IT và Kubernetes — `cap_drop: [ALL]`, `security_opt: [no-new-privileges]`. PostgreSQL chạy bằng user `postgres` của image với `cap_add` tối thiểu (CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID) và `read_only`; provisioner chạy root chỉ để chown volume `run`. JVM option theo `values.schema.json` (`-XX:+ExitOnOutOfMemoryError`; heap mặc định `-Xms256m -Xmx384m`, Task 10 đo và chỉnh). Restart policy: `on-failure:5` cho service, `no` cho gateway (khởi động lại gateway vô nghĩa, §4.5) và provisioner.

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

Cột "Trạng thái" phản ánh SP1, SP2 và SP3 tới Task 9; chưa có hàng nào được chứng minh bằng một lần chạy compose thật (Task 10).

| Rủi ro | Trạng thái | Xử lý |
|---|---|---|
| `NativeCompatibility` đọc `...artery.ssl.config.require-mutual-authentication` nhưng conf đặt dưới `ssl.config-ssl-engine`; gate tương thích có thể không bao giờ mở | **Đã sửa (SP1)**: lỗi đường dẫn khóa là có thật; nơi đọc dùng khóa hiệu lực `ssl.config-ssl-engine`, `ssl.config` chỉ là dự phòng khi thiếu | Có test; gate mở trong `LocalRuntimeCompositionIT` |
| Epoch phát hành của Identity so với watermark `RevocationState` (nguy cơ epoch 0 bị coi là đã revoke) | **Đã giải quyết bằng test (SP2)**: token không có claim `securityEpoch` được đọc là epoch 0; user không có sự kiện nào không bị ảnh hưởng, còn revoke đầu tiên (epoch 0) chặn token epoch 0 | `RevocationPageServiceTest`, `LocalPlatformCompositionIT` (revoke trả epoch 0, user bị từ chối trên các plane đang chạy, user khác không bị); không đổi ngữ nghĩa `RevocationState`. Revoke user là vĩnh viễn trong môi trường này |
| Control cần `CellRpcClient`; `NativeCellRpcConfiguration` hiện chỉ profile `actor`, `gateway` | **Đã giải quyết (SP1)** bằng danh tính workload `control` chỉ-quan-sát (§4.3 mục 7, §4.6) | Test từ chối của server actor cho execute, relay và sync; `LocalRuntimeCompositionIT` chứng minh gate control quay về 503 khi chỉ dừng actor (§9) |
| Cổng thời gian 30s của actor khi khởi động | **Đã xử lý trong thiết kế (SP3)**: không actor nào `depends_on` actor khác, cả mười hai khởi động đồng thời; chưa chạy thật | Nếu compose chậm, tăng giới hạn trong chính config có kiểm range, không bỏ kiểm tra; Task 10 quan sát |
| Phép đo đồng hồ chỉ trong container | **Giới hạn đã chấp nhận (SP2)**: source đo thật lệch wall/monotonic của chính process (`HostClockObserver`) và cộng phụ cấp cố định 100000µs cho độ trễ đọc của consumer | Không dùng làm bằng chứng kỷ luật đồng hồ host; đo trên 5s gần nhất, không thấy drift làm hai đồng hồ lệch cùng nhau (xem `config/native-sources.md`). `ClockReportServiceTest`, `HostClockObserverTest`, `LocalPlatformCompositionIT`. Source của IT SP1 vẫn không đo gì, chỉ khai mức tối đa đã enroll |
| Control giữ credential hai cell | Chấp nhận, chỉ `LOCAL_TEST_ONLY`; SP1 giới hạn còn đúng một `transferCells` | Đóng bằng A3/A4 |
| Cổng RPC ingress của actor (`CellRpcServer`) và relay của gateway (`GatewayRelayRpcServer`) bind mọi interface; WSS và HTTPS của control cũng vậy (chỉ Artery, management và health bind `podIp`) | **Đã xử lý trong thiết kế (SP3)**: mạng cell `internal: true`, cổng RPC/relay/WSS/HTTPS chỉ nằm trên `net-shared`, cổng publish bind `127.0.0.1`; nghiệm thu bằng probe cô lập ở Task 10 | Thiết kế mạng compose cô lập: mỗi cổng chỉ có đường tới từ đúng mạng (§3.2); không dựa vào việc listener tự giới hạn |
| Release Helm `local-minikube` mở cổng kích hoạt vì chart luôn đặt `SIGNALING_NATIVE_PROVIDER_CONTRACT`, nhưng không render `process.json` | **Giới hạn đã chấp nhận (SP1)**: đóng khi lỗi với `LOCAL_RUNTIME_REJECTED` `profile/MISSING` (trước đó `NATIVE_RUNTIME_NOT_INSTALLED`); `SIGNALING_POD_UID` từ `metadata.uid` cũng không bao giờ bằng `podUid` của profile | Adapter chỉ dành cho compose; SP3 sở hữu việc sinh profile và contract. Không sửa chart; chế độ local của Helm cần một quyết định chart riêng sau này |
| Bằng chứng khởi động từ `-exec.jar` | **Còn lại (SP3)** | `LocalRuntimeCompositionIT` chạy các plane trong JVM con với classpath của module, không phải jar đóng gói. `image-contract.sh` chứng minh từ image đóng gói chỉ trường hợp contract thiếu (dòng `LOCAL_RUNTIME_REJECTED`, `role=contract`, `category=MISSING`); khởi động đầy đủ với contract hợp lệ là nghiệm thu của SP3 |
| Chromium thiếu trong môi trường chạy IT | Môi trường: chỉ `WebRtcInteropIT` cần Chromium (`CHROMIUM_PATH`); thiếu thì IT đó fail, không liên quan SP1 | Cài Chromium khi chạy toàn bộ `signaling-integration-tests` |
| `bootId` của gateway ghim cho mỗi lần provision; `--if-needed` bỏ qua giai đoạn đã xong | Giới hạn đã chấp nhận (§4.5) | Khởi động lại riêng gateway không hỗ trợ; phục hồi bằng `make reset` rồi `make up`; `make down` luôn `-v`; `guard.sh` từ chối `make up` trên trạng thái sót lại |
| Thế thân từ chối cho privacy detachment và epoch adoption; admission ghi directory của control là hằng `true` | Giới hạn đã chấp nhận (§4.3) | Không có xóa dữ liệu riêng tư, promotion hay migration trong compose; một control duy nhất |
| Jar `signaling-app` trong `target/` có thể cũ | **Đã xử lý (SP3)** | `make build` chạy `./mvnw -o -pl signaling-app,signaling-identity,signaling-local-platform -am package -DskipTests -Dspotless.check.skip=true` trước khi dựng năm image; image provisioner nhúng `deploy/compose` lúc build nên cũng phải dựng lại bằng `make build` sau khi sửa |
| Tài nguyên (17 JVM + 5 PostgreSQL; tải CPU cao làm flake deadline DB) | **Đã xử lý trong thiết kế (SP3)**; số đo là việc của Task 10 | `preflight.sh` từ chối khi RAM trống dưới 12 GiB (ước lượng) hoặc tải trên 0,7 mỗi core; healthcheck có `start_period` 90s; `make up` chờ tối đa 600s; không hạ deadline production |
| `DbPools` đặt `connectionTimeout` 500 ms cố định trong mã (`HikariConfig.setConnectionTimeout(500)`) | Giới hạn đã chấp nhận (SP3) | Compose không đổi được giá trị này (không có cấu hình); vì vậy tải host cao làm lấy kết nối DB thất bại và là lý do của kiểm tra tải trong preflight. Không sửa mã production cho môi trường local |

## 9. Tiêu chí chấp nhận

1. Từ checkout sạch, `docker compose up` đưa mọi service đến `healthy`; mười hai actor `Up`; `/ready` của gateway, control và actor trả 200.
2. `smoke.mjs` PASS, gồm cross-cell và revoke.
3. Dừng một actor: cell vẫn `ready` (quorum 4) và hồi phục khi khởi động lại. Dừng standby của một cell: lệnh critical của cell đó fail-closed, không có fallback bất đồng bộ.
4. Fingerprint lệch, certificate sai SPIFFE hoặc thiếu contract: container từ chối khởi động với lỗi rõ.
5. `./mvnw verify` của SP1/SP2 xanh; image triển khai không chứa `TestOnlyNativeCapabilities`.
6. Không có secret hay private key mới trong git; mọi artifact và log in `LOCAL_TEST_ONLY` và `NOT_QUALIFIED`.

Bằng chứng mức SP1, không thay thế tiêu chí nào ở trên: `LocalRuntimeCompositionIT` (`signaling-integration-tests`) dựng **một** cell từ file mount và môi trường, với sáu actor, gateway và control là các JVM riêng, PostgreSQL 17.6 thật (primary và standby streaming đồng bộ) và Pekko mTLS. Năm test của nó chứng minh: các plane khởi động chỉ từ file mount và môi trường, không có lớp test trên classpath; listener actor từ chối TLS 1.2; một `INVITE` có xác thực được commit và gate tương thích của control phụ thuộc vào quan sát thật của actor (chứng minh theo chiều ngược: chỉ dừng sáu actor thì control trả về 503 trong khi nguồn đồng hồ và revocation vẫn hoạt động; chiều xuôi, 503 trước actor rồi 200 sau, phù hợp với điều đó nhưng riêng nó không loại trừ một nguồn chậm); cell chỉ có một vùng vẫn chưa sẵn sàng và nêu vùng thiếu; thiếu contract bị từ chối với vai trò và loại lỗi (tiêu chí 4, phần "thiếu contract", ở mức process). `image-contract.sh` chứng minh cùng kết quả từ image đóng gói. **Chưa được chứng minh**: hai cell, gọi chéo cell, chuyển bucket thật, phần còn lại của luồng cuộc gọi, mất actor hay standby, chất lượng đồng hồ, khởi động đầy đủ từ `-exec.jar`; các mục đó thuộc SP2/SP3 và tiêu chí 1–3.

Đạt 1–6 chỉ cho phép trạng thái `LOCAL_QUALIFIED`. Production, HA thật, physical fencing, N−1 và 10M vẫn `NOT_QUALIFIED`.

Trạng thái nghiệm thu (cập nhật Task 9): SP3 triển khai tới Task 9; **nghiệm thu 1–6 là NOT_RUN**. Chưa có lần `make up` nào trong repo này chạy hết stack, nên không tiêu chí nào ở trên được tuyên bố đạt và môi trường chưa là `LOCAL_QUALIFIED`; chỉ lần chạy nghiệm thu của Task 10 (số đo RAM, các bước giả định chưa kiểm trực tiếp của smoke) có thể điền kết quả vào đây.
