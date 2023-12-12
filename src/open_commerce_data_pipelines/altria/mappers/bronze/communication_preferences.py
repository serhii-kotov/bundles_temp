from pyspark.sql import DataFrame


class CommunicationPreferencesBronzeMapper:
    @staticmethod
    def map_to_benchmark_metrics_export(src: DataFrame):
        return src.select(src.customerUuid.alias("customer_id"),
                          (src.emailOptInAt.isNotNull() & src.emailOptOutAt.isNull()).alias("is_email_contactable"))
