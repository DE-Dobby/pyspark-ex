from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pipeline_logger import get_logger

log = get_logger("03.load")

spark = SparkSession.active()


@dp.materialized_view(comment="[적재] 고객별 구매 통계")
def customer_stats() -> DataFrame:
    log.info("적재-1 시작: 고객별 집계")
    log.info("  집계 기준: customer")
    log.info("  집계 항목: 주문 수, 총액, 평균액, 최근 주문일, 티어 분포")

    df = (
        spark.read.table("enriched_orders")
        .groupBy("customer")
        .agg(
            F.count("*")                          .alias("order_count"),
            F.sum("amount")                       .alias("total_amount"),
            F.round(F.avg("amount"), 2)           .alias("avg_amount"),
            F.max("order_date")                   .alias("last_order_date"),
            F.count_if(F.col("amount_tier") == "premium") .alias("premium_count"),
        )
        .orderBy(F.col("total_amount").desc())
    )

    log.info("적재-1 완료: customer_stats 정의")
    return df


@dp.materialized_view(comment="[적재] 상품별 판매 통계")
def product_stats() -> DataFrame:
    log.info("적재-2 시작: 상품별 집계")
    log.info("  집계 기준: product, region")
    log.info("  집계 항목: 판매 수, 총 매출, 주말 판매 비율")

    df = (
        spark.read.table("enriched_orders")
        .groupBy("product", "region")
        .agg(
            F.count("*")                          .alias("sales_count"),
            F.sum("amount")                       .alias("total_revenue"),
            F.round(
                F.avg(F.col("is_weekend").cast("int")) * 100, 1
            )                                     .alias("weekend_sales_pct"),
        )
        .orderBy("product", "region")
    )

    log.info("적재-2 완료: product_stats 정의")
    return df
