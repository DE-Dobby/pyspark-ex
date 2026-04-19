from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType,
)
from pipeline_logger import get_logger

log = get_logger("01.ingest")

spark = SparkSession.active()

_SCHEMA = StructType([
    StructField("order_id",    IntegerType(), nullable=False),
    StructField("customer",    StringType(),  nullable=True),
    StructField("product",     StringType(),  nullable=True),
    StructField("amount",      DoubleType(),  nullable=True),
    StructField("order_date",  StringType(),  nullable=True),
    StructField("region",      StringType(),  nullable=True),
])

_DATA = [
    (1,  "alice", "laptop",   1200.0, "2024-01-10", "KR"),
    (2,  "bob",   "mouse",      35.0, "2024-01-11", "KR"),
    (3,  "alice", "keyboard",   89.0, "2024-01-11", "US"),
    (4,  "carol", "monitor",   450.0, "2024-01-12", "US"),
    (5,  "bob",   "laptop",   1200.0, "2024-01-13", "KR"),
    (6,  "carol", "mouse",      35.0, "2024-01-14", "JP"),
    (7,  "alice", "headset",   200.0, "2024-01-14", "JP"),
    (8,   None,   "keyboard",   89.0, "2024-01-15", "KR"),  # customer 누락
    (9,  "dave",  "monitor",   450.0, "2024-01-15",  None),  # region 누락
    (10, "carol", "laptop",   1200.0, "2024-01-16", "US"),
]


@dp.materialized_view(
    comment="[수집] 원천 주문 데이터 — 가공 없이 보존",
    schema=_SCHEMA,
)
def raw_orders() -> DataFrame:
    log.info("수집 시작: 주문 원천 데이터 로드")
    log.info(f"  스키마: {[f.name for f in _SCHEMA.fields]}")
    log.info(f"  레코드 수(예상): {len(_DATA)}")

    df = spark.createDataFrame(_DATA, schema=_SCHEMA)

    log.info("수집 완료: raw_orders 정의")
    return df
