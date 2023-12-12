from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, lit


class FuelAndConvenienceProfilesBronzeMapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame):
        return src.select(src.membership_id.alias("loyalty_member_id"),
                          coalesce(src.restriction_verification.contains("eaiv"),
                                   lit(False))
                          .alias("is_member_verified"))
