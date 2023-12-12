from datetime import date

from pyspark.pandas.spark.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import count_distinct, col, when, coalesce


class BenchmarkMetricsMergeController:
    @staticmethod
    def merge(communication_preferences: DataFrame, fuel_and_convenience_profiles: DataFrame,
              locations: DataFrame, members: DataFrame, products: DataFrame, management_account_number: str,
              quarter_end: date, quarter_start: date, transaction_line_items: DataFrame):
        member_statistics = (members
                             .join(communication_preferences, "customer_id", "left")
                             .join(fuel_and_convenience_profiles, "loyalty_member_id", "left")
                             .select("member_id",
                                     col("is_app_user").alias("is_app_user"),
                                     col("is_email_contactable").eqNullSafe(True).alias("is_email_contactable"),
                                     col("is_member_verified").eqNullSafe(True).alias("is_member_verified"))
                             .withColumn("management_account_number", lit(management_account_number)))

        member_transaction_statistics = (member_statistics
                                         .join(transaction_line_items, "member_id", "left")
                                         .join(products, "product_id", "left")
                                         .join(locations, "location_id", "left")
                                         .select("rcn_or_man",
                                                 "management_account_number",
                                                 "member_id",
                                                 "transaction_id",
                                                 "loyalty_transaction_time_at",
                                                 "is_app_user",
                                                 "is_email_contactable",
                                                 "is_member_verified",
                                                 col("is_cigarette").eqNullSafe(True).alias("is_cigarette"),
                                                 col("is_cigar").eqNullSafe(True).alias("is_cigar"),
                                                 col("is_mst").eqNullSafe(True).alias("is_mst"),
                                                 col("is_onp").eqNullSafe(True).alias("is_onp"))
                                         .withColumn("rcn_or_man", coalesce(col("rcn_or_man"),
                                                                            col("management_account_number"))))

        return (member_transaction_statistics
                .groupBy("rcn_or_man")
                .agg(count_distinct("member_id").alias("members_on_database"),
                     (count_distinct(when(col("transaction_id").isNotNull(),
                                          col("member_id"))))
                     .alias("tobacco_members_on_database"),
                     (count_distinct(when(col("transaction_id").isNotNull() & col("is_member_verified"),
                                          col("member_id"))))
                     .alias("eaiv_verified_tobacco_members"),
                     (count_distinct(when(col("transaction_id").isNotNull() & col("is_email_contactable"),
                                          col("member_id"))))
                     .alias("email_tobacco_contactable_members"),
                     (count_distinct(when(col("transaction_id").isNotNull() & col("is_app_user"),
                                          col("member_id"))))
                     .alias("app_registered_tobacco_members"),
                     (count_distinct(when(col("transaction_id").isNotNull() &
                                          col("loyalty_transaction_time_at").between(quarter_start, quarter_end),
                                          col("member_id"))))
                     .alias("active_tobacco_app_users"),
                     (count_distinct(when(col("is_cigarette"), col("member_id"))))
                     .alias("cigarette_members_on_database"),
                     (count_distinct(when(col("is_cigarette") & col("is_member_verified"), col("member_id"))))
                     .alias("eaiv_verified_cigarette_members"),
                     (count_distinct(when(col("is_cigarette") & col("is_email_contactable"), col("member_id"))))
                     .alias("email_cigarette_contactable_members"),
                     (count_distinct(when(col("is_cigarette") & col("is_app_user"), col("member_id"))))
                     .alias("app_registered_cigarette_members"),
                     (count_distinct(when(col("is_cigarette") &
                                          col("loyalty_transaction_time_at").between(quarter_start, quarter_end),
                                          col("member_id"))))
                     .alias("active_cigarette_app_users"),
                     (count_distinct(when(col("is_cigar"), col("member_id"))))
                     .alias("cigar_members_on_database"),
                     (count_distinct(when(col("is_cigar") & col("is_member_verified"), col("member_id"))))
                     .alias("eaiv_verified_cigar_members"),
                     (count_distinct(when(col("is_cigar") & col("is_email_contactable"), col("member_id"))))
                     .alias("email_cigar_contactable_members"),
                     (count_distinct(when(col("is_cigar") & col("is_app_user"), col("member_id"))))
                     .alias("app_registered_cigar_members"),
                     (count_distinct(when(col("is_cigar") &
                                          col("loyalty_transaction_time_at").between(quarter_start, quarter_end),
                                          col("member_id"))))
                     .alias("active_cigar_app_users"),
                     (count_distinct(when(col("is_mst"), col("member_id"))))
                     .alias("mst_members_on_database"),
                     (count_distinct(when(col("is_mst") & col("is_member_verified"), col("member_id"))))
                     .alias("eaiv_verified_mst_members"),
                     (count_distinct(when(col("is_mst") & col("is_email_contactable"), col("member_id"))))
                     .alias("email_mst_contactable_members"),
                     (count_distinct(when(col("is_mst") & col("is_app_user"), col("member_id"))))
                     .alias("app_registered_mst_members"),
                     (count_distinct(when(col("is_mst") &
                                          col("loyalty_transaction_time_at").between(quarter_start, quarter_end),
                                          col("member_id"))))
                     .alias("active_mst_app_users"),
                     (count_distinct(when(col("is_onp"), col("member_id"))))
                     .alias("onp_members_on_database"),
                     (count_distinct(when(col("is_onp") & col("is_member_verified"), col("member_id"))))
                     .alias("eaiv_verified_onp_members"),
                     (count_distinct(when(col("is_onp") & col("is_email_contactable"), col("member_id"))))
                     .alias("email_onp_contactable_members"),
                     (count_distinct(when(col("is_onp") & col("is_app_user"), col("member_id"))))
                     .alias("app_registered_onp_members"),
                     (count_distinct(when(col("is_onp") &
                                          col("loyalty_transaction_time_at").between(quarter_start, quarter_end),
                                          col("member_id"))))
                     .alias("active_onp_app_users"),
                     lit(0).alias("email_contactable_members"),
                     lit(0).alias("app_registered_members_on_database"),
                     lit(0).alias("active_app_users"),
                     lit(0.0).alias("email_deliverability_rate"),
                     lit(0.0).alias("email_click_through_rate"),
                     lit(0.0).alias("email_redemption_rate"),
                     lit(0.0).alias("app_avg_impressions"),
                     lit(0.0).alias("app_avg_time_in_app"),
                     lit(0.0).alias("app_click_through_rate"),
                     lit(0.0).alias("app_load_to_card_rate"),
                     lit("").alias("reserved_1"),
                     lit("").alias("reserved_2"),
                     lit("").alias("reserved_3"),
                     lit("").alias("reserved_4"),
                     lit("").alias("reserved_5"),
                     lit("").alias("reserved_6"),
                     lit("").alias("reserved_7")))
