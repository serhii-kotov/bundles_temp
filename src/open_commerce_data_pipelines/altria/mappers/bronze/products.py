from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains


class ProductsBronzeStage1Mapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame):
        return src.select(src.id.alias("product_id"),
                          array_contains(src.categories, "cigarette").alias("is_cigarette"),
                          array_contains(src.categories, "cigar").alias("is_cigar"),
                          (array_contains(src.categories, "smokeless")).alias("is_mst"),
                          (array_contains(src.categories, "smoking cessation") |
                           array_contains(src.categories, "electronic") |
                           array_contains(src.categories, "accessory")).alias("is_onp"))
