from pyspark.sql import DataFrame


class MembersSilverStage1Mapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame):
        return src.select(src.id.alias("member_id"),
                          "customer_id",
                          "loyalty_member_id",
                          (src.loyalty_status_current == "ACTIVE").alias("is_app_user"),
                          "last_loyalty_transaction_at")
