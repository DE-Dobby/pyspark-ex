from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pipeline_logger import get_logger

log = get_logger("02.transform")

spark = SparkSession.active()


@dp.temporary_view
def cleaned_orders() -> DataFrame:
    log.info("변환-1 시작: null 제거")
    log.info("  조건: customer IS NOT NULL AND region IS NOT NULL")

    df = (
        spark.read.table("raw_orders")
        .filter(F.col("customer").isNotNull() & F.col("region").isNotNull())
    )

    log.info("변환-1 완료: cleaned_orders 정의")
    return df


@dp.materialized_view(comment="[변환-2] 중복 제거 + 타입 변환 + 비즈니스 컬럼 추가")
def enriched_orders() -> DataFrame:
    log.info("변환-2 시작: 중복 제거 + 비즈니스 컬럼 추가")
    log.info("  중복 기준: order_id")
    log.info("  amount_tier: >=1000 → premium / >=100 → standard / else → basic")
    log.info("  is_weekend: dayofweek IN (1=일, 7=토)")

    df = spark.sql("""
        SELECT
            order_id,
            customer,
            product,
            amount,
            to_date(order_date, 'yyyy-MM-dd')   AS order_date,
            region,
            CASE
                WHEN amount >= 1000 THEN 'premium'
                WHEN amount >= 100  THEN 'standard'
                ELSE 'basic'
            END                                  AS amount_tier,
            dayofweek(order_date) IN (1, 7)      AS is_weekend
        FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
            FROM cleaned_orders
        )
        WHERE rn = 1
    """)

    log.info("변환-2 완료: enriched_orders 정의")
    return df
