from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit


class LocationsSilverStage1Mapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame, management_account_number):
        return (src.withColumn("rcn_or_man",
                               when(col("altria_rcn").isNotNull(), col("altria_rcn")).otherwise(
                                   lit(management_account_number)))
                .select(src.id.alias("location_id"),
                        "rcn_or_man"))
