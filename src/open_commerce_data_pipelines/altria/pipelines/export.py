import abc
import codecs
from abc import ABC
from ctypes import ArgumentError
from typing import Any, Optional, cast

from pyarrow.fs import FileSystem
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, exists, array_contains
from pyspark.sql.functions import lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from sprak.io import IoResourceFormats
from sprak.io.file import FileIoOutputModes

from open_commerce_data_pipelines.altria.controllers.benchmark_metrics import BenchmarkMetricsMergeController
from open_commerce_data_pipelines.altria.mappers.bronze.communication_preferences import \
    CommunicationPreferencesBronzeMapper
from open_commerce_data_pipelines.altria.mappers.bronze.fuel_and_convenience_profiles import \
    FuelAndConvenienceProfilesBronzeMapper
from open_commerce_data_pipelines.altria.mappers.bronze.offer_activity_log import OfferActivityLogBronzeStage1Mapper
from open_commerce_data_pipelines.altria.mappers.bronze.products import ProductsBronzeStage1Mapper
from open_commerce_data_pipelines.altria.mappers.silver.locations import LocationsSilverStage1Mapper
from open_commerce_data_pipelines.altria.mappers.silver.members import MembersSilverStage1Mapper
from open_commerce_data_pipelines.altria.mappers.silver.transactions import TransactionsSilverStage1Mapper
from open_commerce_data_pipelines.core.pipelines.export import DailyExtractPipeline, \
    MonthlyExtractPipelineConfig, QuarterlyExtractPipelineConfig, MonthlyExtractPipeline, QuarterlyExtractPipeline, \
    DailyExtractPipelineConfig
from open_commerce_data_pipelines.core.schemas.models.bronze import OFFER_ACTIVITY_LOG_BRONZE_STAGE_1_SCHEMA
from open_commerce_data_pipelines.core.schemas.models.silver.transactions import TRANSACTIONS_SILVER_STAGE_1_SCHEMA


class DigitalReportingExtractPipelineConfig(DailyExtractPipelineConfig):
    __scan_data_prefix: Optional[str]
    __management_account_number: Optional[str]
    __email_address: Optional[str]

    def __init__(self, email_address: Optional[str] = None, scan_data_prefix: Optional[str] = None,
                 management_account_number: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        if not email_address:
            raise ArgumentError("email_address is required")

        if not management_account_number:
            raise ArgumentError("management_account_number is required")

        if not scan_data_prefix:
            raise ArgumentError("scan_data_prefix is required")

        self.__email_address = email_address
        self.__management_account_number = management_account_number
        self.__scan_data_prefix = scan_data_prefix

    @property
    def email_address(self) -> str:
        return self.__email_address

    @property
    def management_account_number(self) -> str:
        return self.__management_account_number

    @property
    def scan_data_prefix(self) -> str:
        return self.__scan_data_prefix


class DigitalReportingExtractPipeline(DailyExtractPipeline, ABC):
    output_format = IoResourceFormats.TEXT
    output_mode = FileIoOutputModes.OVERWRITE
    partition_by = ["timestamp_id"]

    @abc.abstractmethod
    def get_timestamp(self) -> str:
        raise NotImplementedError()

    def before_save(self, src: DataFrame) -> DataFrame:
        email_address = self.config.email_address
        # Add the summary line to the DataFrame
        summary_line = f"{src.count()}|{email_address}"

        # Concatenate all fields within the source.
        # NOTE: this will concatenate all fields in the order in which
        # they occur in the DataFrame. This means that we need to be
        # sure to pass the DataFrame to the save function with the proper
        # column ordering.
        modified = src.select(lit(1).alias("idx"),
                              concat_ws("|", *src.schema.fieldNames()))

        return (self
                .sc
                .parallelize([Row(idx=0, value=summary_line)])
                .toDF()
                .union(modified)
                .withColumn("timestamp_id", lit(self.get_timestamp()))
                .coalesce(1)
                .sortWithinPartitions("idx")
                .drop("idx", "timestamp_id"))

    def save(self, src: DataFrame, **save_options: Any) -> Optional[StreamingQuery]:
        """Save the supplied DataFrame to the configured output path.

        Parameters
        ----------
        src: DataFrame
            The DataFrame to persist.
        save_options: Dict[str, Any]
            A collection of arguments to be used when writing the output."""

        transformed = self.before_save(src)

        file_name = f"{self.output_table.location}/" \
                    f"{self.config.scan_data_prefix}_" \
                    f"{self.output_name}_" \
                    f"{self.get_timestamp()}.dat"

        fs, path = FileSystem.from_uri(file_name)
        with fs.open_output_stream(path) as stream:
            with codecs.getwriter("utf-8")(stream) as wrapped_stream:
                return (wrapped_stream
                        .write(transformed
                               .pandas_api()
                               .to_string(header=False,
                                          index=False,
                                          justify="left",
                                          sparsify=True,
                                          col_space=1)
                               .strip()))

    @property
    def config(self) -> DigitalReportingExtractPipelineConfig:
        return cast(DigitalReportingExtractPipelineConfig, super().config)


class DigitalReportingMonthlyExtractPipelineConfig(DigitalReportingExtractPipelineConfig,
                                                   MonthlyExtractPipelineConfig):
    pass


class DigitalReportingMonthlyExtractPipeline(DigitalReportingExtractPipeline, MonthlyExtractPipeline):
    def __init__(self, config: DigitalReportingMonthlyExtractPipelineConfig) -> None:
        super().__init__(config)

    def get_timestamp(self) -> str:
        last_month = self.config.capture_day_id

        return last_month.strftime("%m%Y")

    @property
    def config(self) -> DigitalReportingMonthlyExtractPipelineConfig:
        return cast(DigitalReportingMonthlyExtractPipelineConfig, super().config)


class DigitalReportingQuarterlyExtractPipelineConfig(DigitalReportingExtractPipelineConfig,
                                                     QuarterlyExtractPipelineConfig):
    pass


class DigitalReportingQuarterlyExtractPipeline(DigitalReportingExtractPipeline, QuarterlyExtractPipeline):
    def __init__(self, config: DigitalReportingQuarterlyExtractPipelineConfig) -> None:
        super().__init__(config)

    def get_timestamp(self) -> str:
        last_quarter = (self.config.capture_day_id.month - 1) // 3 + 1
        year = self.config.capture_day_id.year

        return f"{last_quarter}Q{year}"

    @property
    def config(self) -> DigitalReportingQuarterlyExtractPipelineConfig:
        return cast(DigitalReportingQuarterlyExtractPipelineConfig, super().config)


class ActivityLogExtractPipeline(DigitalReportingMonthlyExtractPipeline):
    output_name = "CampaignActivityLog"
    input_name = "offer-activity-log-stage-1"
    input_schema = OFFER_ACTIVITY_LOG_BRONZE_STAGE_1_SCHEMA
    output_schema = (StructType().add("value", "string")
                                 .add("timestamp_id", "timestamp"))
    timestamp_column = "updatedAt"
    capture_column_name = "timestamp_id"

    def mapper_function(self, src: DataFrame):
        return OfferActivityLogBronzeStage1Mapper.map_to_activity_log_export(src, self.config.management_account_number)


class BenchmarkMetricsExtractPipelineConfig(DigitalReportingQuarterlyExtractPipelineConfig):
    __communication_preferences_input_path: Optional[str]
    __fuel_and_convenience_profiles_input_path: Optional[str]
    __members_input_path: Optional[str]
    __products_input_path: Optional[str]
    __locations_input_path: Optional[str]

    def __init__(self, communication_preferences_input_path: Optional[str] = None,
                 fuel_and_convenience_profiles_input_path: Optional[str] = None,
                 members_input_path: Optional[str] = None, products_input_path: Optional[str] = None,
                 locations_input_path: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        if not communication_preferences_input_path:
            raise ArgumentError("communication_preferences_input_path is required")

        if not fuel_and_convenience_profiles_input_path:
            raise ArgumentError("fuel_and_convenience_profiles_input_path is required")

        if not members_input_path:
            raise ArgumentError("members_input_path is required")

        if not products_input_path:
            raise ArgumentError("products_input_path is required")

        if not locations_input_path:
            raise ArgumentError("locations_input_path is required")

        self.__communication_preferences_input_path = communication_preferences_input_path
        self.__fuel_and_convenience_profiles_input_path = fuel_and_convenience_profiles_input_path
        self.__members_input_path = members_input_path
        self.__products_input_path = products_input_path
        self.__locations_input_path = locations_input_path

    @property
    def communication_preferences_input_path(self):
        return self.__communication_preferences_input_path

    @property
    def fuel_and_convenience_profiles_input_path(self):
        return self.__fuel_and_convenience_profiles_input_path

    @property
    def members_input_path(self):
        return self.__members_input_path

    @property
    def products_input_path(self):
        return self.__products_input_path

    @property
    def locations_input_path(self):
        return self.__locations_input_path


class BenchmarkMetricsExtractPipeline(DigitalReportingQuarterlyExtractPipeline):
    output_name = "MemberBenchmark"
    input_name = "transactions-stage-1"
    input_schema = TRANSACTIONS_SILVER_STAGE_1_SCHEMA
    output_schema = (StructType().add("value", "string")
                                 .add("timestamp_id", "timestamp"))
    timestamp_column = "loyalty_transaction_time_at"
    capture_column_name = "timestamp_id"

    def __init__(self, config: BenchmarkMetricsExtractPipelineConfig) -> None:
        super().__init__(config)

    @property
    def communication_preferences(self):
        return (self
                .spark
                .read
                .option("path", self.config.communication_preferences_input_path)
                .format("delta")
                .load())

    @property
    def config(self) -> BenchmarkMetricsExtractPipelineConfig:
        return cast(BenchmarkMetricsExtractPipelineConfig, super().config)

    def filter_function(self, src: DataFrame) -> DataFrame:
        return (super()
                .filter_function(src)
                .where(exists("line_items",
                              lambda x: x.product_payment_systems_code.between("410", "419"))))

    @property
    def fuel_and_convenience_profiles(self):
        return (self
                .spark
                .read
                .option("path", self.config.fuel_and_convenience_profiles_input_path)
                .format("delta")
                .load())

    def mapper_function(self, src: DataFrame):
        return TransactionsSilverStage1Mapper.map_to_benchmark_metrics_export(src)

    @property
    def members(self):
        return (self
                .spark
                .read
                .option("path", self.config.members_input_path)
                .format("delta")
                .load())

    @property
    def products(self):
        return (self
                .spark
                .read
                .option("path", self.config.products_input_path)
                .format("delta")
                .load())

    @property
    def locations(self):
        return (self
                .spark
                .read
                .option("path", self.config.locations_input_path)
                .format("delta")
                .load())

    def run(self):
        communication_preferences = (CommunicationPreferencesBronzeMapper
                                     .map_to_benchmark_metrics_export(self.communication_preferences))
        fuel_and_convenience_profiles = (FuelAndConvenienceProfilesBronzeMapper
                                         .map_to_benchmark_metrics_export(self.fuel_and_convenience_profiles))
        members = MembersSilverStage1Mapper.map_to_benchmark_metrics_export(self.members)
        products = ProductsBronzeStage1Mapper.map_to_benchmark_metrics_export(self._filter_products())
        locations = (LocationsSilverStage1Mapper.map_to_benchmark_metrics_export(self.locations,
                                                                                 self.config.management_account_number))
        transaction_line_items = self.mapper_function(self.input_model)

        merged = BenchmarkMetricsMergeController.merge(communication_preferences, fuel_and_convenience_profiles,
                                                       locations, members, products,
                                                       self.config.management_account_number,
                                                       self.quarter_end, self.quarter_start, transaction_line_items)

        return self.save(merged)

    def _filter_products(self):
        return self.products.where(array_contains("categories", "tobacco"))
