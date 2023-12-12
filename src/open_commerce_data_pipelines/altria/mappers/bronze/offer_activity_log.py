from pyspark.sql import DataFrame
from pyspark.sql.functions import date_format, substring, lit


class OfferActivityLogBronzeStage1Mapper:
    @staticmethod
    def map_to_activity_log_export(src: DataFrame, management_account_number):
        return src.select(lit(management_account_number).alias("management_account_number"),
                          date_format(src.updatedAt, "yyyyMMdd").alias("activity_date"),
                          date_format(src.updatedAt, "HH:mm:ss").alias("activity_time"),
                          substring(src.adId, 1, 20).alias("ad_id"),
                          substring(src.membershipId, 1, 20).alias("loyalty_id"),
                          substring(src.activityType, 1, 20).alias("activity_type"),
                          substring(src.channel, 1, 20).alias("channel_type"),
                          substring(src.marketingTransactionLogId, 1, 50).alias("marketing_transaction_id"),
                          substring(src.activityState, 1, 2).alias("activity_state"))
