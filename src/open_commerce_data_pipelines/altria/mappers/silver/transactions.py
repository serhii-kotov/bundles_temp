from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col


class TransactionsSilverStage1Mapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame):
        return (src
                .select(src.id.alias("transaction_id"),
                        "member_id",
                        "location_id",
                        "loyalty_transaction_time_at",
                        explode("line_items").alias("line_item"))
                .select("transaction_id",
                        "member_id",
                        "location_id",
                        "loyalty_transaction_time_at",
                        "line_item.*")
                .where(col("product_payment_systems_code").between("410", "419"))
                .select("transaction_id",
                        "member_id",
                        "location_id",
                        "loyalty_transaction_time_at",
                        "product_id"))
