# ai-pipeline-ec2

추천: 제목은 "AI Data Pipelines on EC2"(또는 국문 "AI 데이터 파이프라인")로 가장 짧고 직관적으로 가고, CPU/Graviton·self-managed 같은 소비 포인트는 부제로 빼세요. 예:

AI Data Pipelines on EC2 — Airflow 기반 데이터·클래식 ML 파이프라인을 Graviton/EC2에서 비용 효율적으로

이러면 고객에겐 "AI 데이터"로 매력적이고(끌어오기), 내부적으론 CPU·EC2 소비라는 실체가 부제에 담깁니다. ('foundation'보다 'pipelines'가 "뭘 하는지"가 즉시 보여서 더 쉽게 읽혀요.)


수평 — AI Data Pipelines on EC2 (기술 기반). AI를 도입·확장하며 그 토대가 되는 데이터·클래식 ML 파이프라인을 관리형·서버리스 대신 EC2에서 직접 운영하려는 고객이 대상이다. AI의 헤드라인은 GPU 위의 모델이지만, 그 모델을 먹여 살리는 데이터 파이프라인 — ETL·피처 생성·배치 추론 — 은 CPU에서 돈다. 포인트는 이 처리를 EMR·Batch·Glue 같은 별도·서버리스 서비스가 아니라 고객이 직접 띄운 EC2에서 실행하는 것이다. 클래식 ML·중간 규모는 고메모리 Graviton 한 대로 스케일업(pandas·XGBoost·Dask)하고, 더 커지면 Dask·Ray·Spark를 EC2에 self-managed로 띄워 스케일아웃하며, Airflow도 EC2 워커에서 태스크를 직접 실행해 처리 전부가 raw EC2 소비로 떨어진다. 스케줄로 반복 처리할수록 CPU 인스턴스-시간이 꾸준히 쌓이고, 데이터와 AI 도입이 깊어질수록 파이프라인도 함께 커져 순증한다. 워크샵(인스턴스 사이징·Graviton·Spot·self-managed 분산)은 대용량 S3·야간 배치·기존 Spark를 가진 고객을 겨냥하며, 워크샵 → PoC → 대규모 반복 파이프라인 상시 운영으로 이어진다. (워크샵: 개발 예정)

