# Spark 4.1.1 Declarative Pipelines 예제

## 환경 설정

Spark 4.1.1은 Python 3.10+ 필요.

```bash
# 가상환경 생성 (최초 1회)
/opt/homebrew/bin/python3.11 -m venv .venv
.venv/bin/pip install pyyaml pandas "pyarrow>=15.0.0" \
    "grpcio>=1.48.1" "grpcio-status>=1.48.1" \
    "googleapis-common-protos>=1.56.4" "protobuf>=3.20.0" \
    "zstandard>=0.25.0"

# 실행 시 환경 변수
export SPARK_HOME=/Users/kds/opt/spark-4.1.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=/Users/kds/workspace/pyspark-ex/.venv/bin/python3
```

---

## 01. Batch Pipeline

수집 → 변환 → 적재 3단계 배치 파이프라인.

```
[수집] ingest.py     raw_orders            (materialized_view, 명시적 스키마)
              ↓
[변환] transform.py  cleaned_orders        (temporary_view,    null 필터)
                     enriched_orders       (materialized_view, 중복 제거 + 파생 컬럼)
              ↓
[적재] load.py       customer_stats        (materialized_view, 고객별 집계)
                     product_stats         (materialized_view, 상품별 집계)
```

### 실행

```bash
cd 01_batch_pipeline
./reset-and-run.sh                       # 실행
./reset-and-run.sh --full-refresh-all    # 전체 재처리
spark-pipelines dry-run                  # 그래프 검증만
```

### 파일 구조

```
01_batch_pipeline/
├── spark-pipeline.yaml       파이프라인 스펙
├── reset-and-run.sh          실행 스크립트
├── pipeline_logger.py        공통 로거
└── transformations/
    ├── ingest.py             [수집] raw_orders
    ├── transform.py          [변환] cleaned_orders, enriched_orders
    └── load.py               [적재] customer_stats, product_stats
```

---