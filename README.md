# onefs-exporter

Dell PowerScale (OneFS) 클러스터를 위한 경량 Prometheus exporter입니다. OneFS REST API(PAPI)를 직접 폴링해서 `/metrics`로 노출합니다. 외부 의존성이 없는 순수 Python 표준 라이브러리로 작성되어 있습니다.

## 왜 이 프로젝트인가

Dell의 공식 [csm-metrics-powerscale](https://github.com/dell/csm-metrics-powerscale)은 Kubernetes CSI 볼륨 관점의 용량/성능 지표만 제공하고(딱 10개 메트릭), Kubernetes 클러스터 안에서만 동작합니다(in-cluster 리더 선출이 코드에 하드코딩되어 있어 순수 컨테이너로는 실행 불가). 노드별 CPU/메모리, 프로토콜별(NFS/SMB/HTTP) I/O, 네트워크 인터페이스별 처리량, Job Engine 상태, 하드웨어 알림/헬스 같은 좀 더 폭넓은 운영 지표가 필요하다면 이 프로젝트를 사용하세요.

- Kubernetes 없이 어디서나 컨테이너 하나로 실행 가능
- OneFS `/platform/3/statistics/current` API를 직접 사용 (10,000개 이상의 통계 키 접근 가능)
- 표준 Prometheus text exposition format

## 아키텍처

```
┌─────────────────┐      REST/HTTPS       ┌──────────────────┐      /metrics       ┌────────────┐
│  PowerScale      │ <──────────────────── │  onefs-exporter  │ <────────────────── │ Prometheus │
│  (OneFS PAPI)    │   basic auth          │  (single binary) │   text exposition   │            │
└─────────────────┘                        └──────────────────┘                     └────────────┘
```

- **큐레이션 지표**: 자주 쓰는 핵심 지표(용량/성능/노드별 CPU·메모리/프로토콜/네트워크/Job Engine/헬스) 30초 주기 폴링
- **전체 카탈로그 지표**(옵션): OneFS가 제공하는 숫자형 통계 키 전체(약 8,000개, `onefs_raw_*` 접두사) 5분 주기 폴링 — 탐색/확장용

## 요구사항

- PowerScale OneFS 클러스터, REST API(기본 포트 8080) 접근 가능
- 읽기 권한을 가진 계정 (통계/이벤트/Job 목록 조회 권한)
- 컨테이너 런타임 (Docker 또는 nerdctl/containerd)

## 빠른 시작

### 1. 이미지 빌드

```bash
docker build -t onefs-exporter:latest .
# 또는 nerdctl
nerdctl build -t onefs-exporter:latest .
```

### 2. 환경변수 설정

`deploy/env.example`을 복사해서 실제 값으로 채우세요.

```bash
cp deploy/env.example deploy/env
vi deploy/env
```

### 3. 실행

```bash
docker run -d --name onefs-exporter \
  --env-file deploy/env \
  -p 9684:9684 \
  --restart unless-stopped \
  onefs-exporter:latest
```

### 4. 확인

```bash
curl http://localhost:9684/metrics
```

## systemd로 실행 (Docker/nerdctl 데몬 없이)

`deploy/onefs-exporter.service.example`을 참고해서 `/etc/systemd/system/onefs-exporter.service`로 설치하세요.

```bash
cp deploy/onefs-exporter.service.example /etc/systemd/system/onefs-exporter.service
cp deploy/env.example /etc/onefs-exporter/env   # 실제 값으로 채운 뒤
systemctl daemon-reload
systemctl enable --now onefs-exporter.service
```

## 설정 (환경변수)

| 변수 | 기본값 | 설명 |
|---|---|---|
| `ONEFS_ENDPOINT` | `onefs.example.com:8080` | OneFS API 엔드포인트 (`host:port`) |
| `ONEFS_USERNAME` | (없음) | OneFS 계정 |
| `ONEFS_PASSWORD` | (없음) | OneFS 계정 비밀번호 |
| `ONEFS_INSECURE` | `true` | `true`면 TLS 인증서 검증 스킵 (자체 서명 인증서 대응) |
| `ONEFS_API_TIMEOUT` | `10` | API 호출 타임아웃(초) |
| `POLL_INTERVAL_SECONDS` | `30` | 큐레이션 지표 폴링 주기(초) |
| `ALL_STATS_ENABLED` | `true` | 전체 카탈로그 지표 수집 여부 |
| `ALL_POLL_INTERVAL_SECONDS` | `300` | 전체 카탈로그 지표 폴링 주기(초) |
| `ALL_BATCH_SIZE` | `200` | 전체 카탈로그 조회 시 API 호출당 키 개수 |
| `LISTEN_PORT` | `9684` | exporter가 리슨할 포트 |

## 제공 지표

### 큐레이션 지표 (`onefs_*`, 항상 켜짐)

| 메트릭 | 설명 |
|---|---|
| `onefs_cluster_capacity_total_bytes` / `_avail_bytes` | 클러스터 전체/가용 용량 |
| `onefs_cluster_health` | 클러스터 헬스: 0=정상, 1=주의, 2=다운 |
| `onefs_cluster_alert_count` | 활성 critical 이상 알림 수 |
| `onefs_cluster_cpu_sys_percent` | 클러스터 평균 system CPU % |
| `onefs_cluster_disk_xfers_in/out_rate`, `_bytes_in/out_rate` | 클러스터 디스크 전송률 |
| `onefs_protocol_op_rate{protocol}` / `_in_rate_bytes` / `_out_rate_bytes` | 프로토콜(nfs/nfs4/smb2/http)별 처리율 |
| `onefs_node_health{node}` | 노드별 헬스 |
| `onefs_node_cpu_idle/sys/user_percent{node}` | 노드별 CPU 사용률 |
| `onefs_node_memory_used/free_bytes{node}` | 노드별 메모리 |
| `onefs_node_net_ext/int_bytes_in/out_rate{node}` | 노드별 네트워크(내부/외부) 처리량 |
| `onefs_job_engine_running_jobs` | 현재 실행 중인 Job Engine 작업 수 |
| `onefs_exporter_scrape_success` / `_last_success_timestamp_seconds` | exporter 자체 상태 |

### 전체 카탈로그 지표 (`onefs_raw_*`, `ALL_STATS_ENABLED=true`일 때)

OneFS `/platform/3/statistics/keys`에서 숫자형(uint64/int32/double/int64) 키를 전부 조회해 `onefs_raw_<key>` 형태로 노출합니다 (약 8,000개 키, 클러스터/노드 스코프 전부 포함). 문자열형 키와 protostats류 복합 객체 키는 현재 제외되어 있습니다.

> 데이터량이 크므로(약 2MB/scrape) 필요한 지표를 확인한 뒤 `ALL_STATS_ENABLED=false`로 끄고 큐레이션 목록에 필요한 키를 추가하는 걸 권장합니다.

## Prometheus 설정 예시

```yaml
scrape_configs:
  - job_name: 'onefs-powerscale'
    scrape_interval: 30s
    static_configs:
      - targets: ['<exporter-host>:9684']
```

## 참고

- 인증은 Basic Auth이며 매 요청마다 자격증명이 사용됩니다.
- PowerScale이 공유 자원인 경우, 전체 카탈로그 폴링 주기(`ALL_POLL_INTERVAL_SECONDS`)를 너무 짧게 잡지 마세요.
- OneFS 통계 API 전체 키 목록은 `GET /platform/3/statistics/keys`로 직접 조회할 수 있습니다.

## 라이선스

MIT License — [LICENSE](LICENSE) 참고
