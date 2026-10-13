# ai-data-pipeline

AI Data Pipelines on EC2 — Airflow 기반 데이터·클래식 ML 파이프라인을 Graviton/EC2에서 비용 효율적으로



수평 — AI Data Pipelines on EC2 (기술 기반). AI를 도입·확장하며 그 토대가 되는 데이터·클래식 ML 파이프라인을 관리형·서버리스 대신 EC2에서 직접 운영하려는 고객이 대상이다. AI의 헤드라인은 GPU 위의 모델이지만, 그 모델을 먹여 살리는 데이터 파이프라인 — ETL·피처 생성·배치 추론 — 은 CPU에서 돈다. 포인트는 이 처리를 EMR·Batch·Glue 같은 별도·서버리스 서비스가 아니라 고객이 직접 띄운 EC2에서 실행하는 것이다. 클래식 ML·중간 규모는 고메모리 Graviton 한 대로 스케일업(pandas·XGBoost·Dask)하고, 더 커지면 Dask·Ray·Spark를 EC2에 self-managed로 띄워 스케일아웃하며, Airflow도 EC2 워커에서 태스크를 직접 실행해 처리 전부가 raw EC2 소비로 떨어진다. 스케줄로 반복 처리할수록 CPU 인스턴스-시간이 꾸준히 쌓이고, 데이터와 AI 도입이 깊어질수록 파이프라인도 함께 커져 순증한다. 워크샵(인스턴스 사이징·Graviton·Spot·self-managed 분산)은 대용량 S3·야간 배치·기존 Spark를 가진 고객을 겨냥하며, 워크샵 → PoC → 대규모 반복 파이프라인 상시 운영으로 이어진다. (워크샵: 개발 예정)



# AI Data Pipelines on EC2

Airflow 기반 **데이터·클래식 ML 파이프라인**을 Graviton/EC2에서 **self-managed**로 운영하는 핸즈온 워크샵.
AI의 토대가 되는 데이터 처리를 관리형이 아니라 **고객이 띄운 EC2에서 직접** 돌린다.

## 개요

| 항목 | 내용 |
|---|---|
| 대상 | AI를 도입·확장하며 데이터·ML 파이프라인을 EC2에서 직접 운영하려는 데이터/플랫폼·ML 엔지니어 |
| 선수지식 | 기본 Linux·EC2, Python/pandas 맛보기 |
| 소요 | 약 3시간 (반나절) |
| 비용 주의 | 큰 CPU 인스턴스를 사용 → 실습은 **Graviton + Spot** 권장, 종료 시 **반드시 리소스 삭제** |

## 핵심 제약 (이 워크샵의 정체성)

모든 처리는 **고객이 띄운 EC2에서** 실행한다.
**EMR · AWS Batch · Glue · EMR Serverless · Fargate · SageMaker Processing은 사용하지 않는다.**
→ 데이터 처리 소비가 전부 **raw EC2**로 떨어진다.

## 아키텍처

```
         +--------------------- EC2 (Graviton) ---------------------+
 (스케줄) |  Airflow (self-managed) --태스크 실행-->  처리 컴퓨트       |
         |   scheduler / worker                   - 스케일업: 큰 EC2 1대 |
         |                                          (pandas/XGBoost/Dask)|
         |                                        - 스케일아웃: Dask/Ray |
         |                                          클러스터 (EC2 직접 구성)|
         +-------+------------------------------------------+---------+
                 | read / write                             |
          [ S3 데이터 레이크 ]  raw -> processed(Parquet) -> features -> model -> predictions
```

EMR/Batch/서버리스 없음. 오케스트레이션도 처리도 전부 EC2.

## 학습 목표

끝나면 다음을 할 수 있다.

1. **self-managed Airflow를 EC2에** 올려 파이프라인을 스케줄링
2. S3 → **ETL → 피처 → XGBoost 학습 → 배치 추론 → S3** 파이프라인 구축
3. 컴퓨트를 EC2에서 직접 — **스케일업(큰 Graviton 1대)**과 **스케일아웃(Dask/Ray on EC2)** 둘 다
4. **비용 최적화**: Graviton vs x86, 배치 Spot, 스케일업 vs 스케일아웃 판단, right-sizing

## 모듈 구성 (핸즈온)

| # | 모듈 | 핵심 내용 | 최적화 포인트 (Compute) |
|---|---|---|---|
| 0 | 개요 & 아키텍처 | EC2 직접 실행 이유, CPU 소비가 어디서 나오나, 피할 서비스 | 스케일업 vs 스케일아웃 의사결정 틀 |
| 1 | 환경 준비 | Graviton EC2 + S3 + **합성 데이터 생성(수 GB 태뷸러)** | 인스턴스 패밀리 선택(컴퓨트 vs 메모리) |
| 2 | Self-managed Airflow on EC2 | 설치·기동, DAG 기본, **태스크를 EC2 워커에서 실행**(LocalExecutor로 시작) | 관리형(MWAA) 대신 EC2에 두는 이유 |
| 3 | ETL + 피처 파이프라인 | S3 raw → 정제·조인·집계 → 피처 → S3 Parquet (pandas/Polars) | 메모리 사이징, Parquet로 I/O 절감 |
| 4 | 클래식 ML 학습 | XGBoost를 EC2에서 학습, 모델 S3 저장 (하이퍼파라미터 튜닝 = 병렬 CPU 부하) | 코어 수 ↔ 튜닝 병렬도, Graviton 성능 |
| 5 | 배치 추론 | 스케줄로 신규 데이터 스코어링 → S3 | 배치는 Spot에 최적 |
| 6 | 스케일아웃 (self-managed) | **Dask(또는 Ray) 클러스터를 EC2에 직접** 구성, 큰 데이터 처리 (EMR 없이) | "클러스터 과잉 vs 큰 1대" 판단, 노드 사이징 |
| 7 | **비용 최적화 (하이라이트)** | Graviton vs x86 벤치, 배치 워커 Spot, right-sizing, CPU 사용률 관찰 | Compute 전문성이 그대로 가치가 되는 지점 |
| 8 | 스케줄 & 운영 | Airflow 스케줄·재시도; (선택) Prometheus/Grafana로 EC2 플릿 모니터링 | GPU 모니터링 워크샵과 크로스링크 |
| - | Cleanup | EC2·S3 정리로 과금 방지 | - |

## 설계 포인트 (왜 이렇게)

- **합성 데이터 생성(Lab 1)**: 실제 대용량 데이터가 없어도 수 GB를 생성해 파이프라인·스케일·CPU 부하를 재현. 재현성·비용 통제에 유리.
- **스케일업 먼저, 스케일아웃 나중**: 큰 Graviton 1대로 끝까지 돌려 성공 경험을 준 뒤(Lab 1~5), 데이터를 키워 Dask로 확장(Lab 6). "클러스터부터 꺼내는" 과잉설계를 교정 — 정직하면서 깨끗한 EC2 소비.
- **CPU가 일하는 걸 눈으로**: Lab마다 `htop`/CloudWatch로 EC2 CPU 사용률을 보여줘 "이게 EC2를 먹는다"를 체감.

## 시작 전 확인

1. **실행 모델**: 단일 큰 EC2(LocalExecutor)로 시작 → Dask/Ray 스케일아웃으로 확장하는 2-트랙? (권장)
2. **포맷**: AWS Workshop Studio 디렉터리 vs 핸즈온 가이드(README+코드)?

## 다음 단계

Lab 1~3을 실제 명령어·DAG 코드·합성 데이터 생성 스크립트까지 완성본으로 작성.
(명령·버전은 작성 시점에 재확인하여 고정)
