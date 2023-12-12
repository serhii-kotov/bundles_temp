from abc import ABC
from ctypes import ArgumentError
from datetime import datetime
from typing import Optional, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from sprak.io import IoResourceFormats
from sprak.io.file import FileIoOutputModes
from sprak.pipelines import SingleSourceDataPipelineConfig

from open_commerce_data_pipelines.altria.mappers.raw import UpcListCigaretteProductMapper, \
    UpcListCigarProductMapper, UpcListSmokelessProductMapper, UpcListTDPProductMapper, UpcListEvaporProductMapper, \
    UpcListTobaccoAccessoriesProductMapper
from open_commerce_data_pipelines.altria.schemas.raw.upc_list import CIGARETTES_TAB_UPC_LIST_SCHEMA, \
    CIGARS_TAB_UPC_LIST_SCHEMA, SMOKELESS_TAB_UPC_LIST_SCHEMA, TDP_TAB_UPC_LIST_SCHEMA, EVAPOR_TAB_UPC_LIST_SCHEMA, \
    TOBACCO_ACCESSORIES_UPC_LIST_SCHEMA
from open_commerce_data_pipelines.core.pipelines.bronze.products import ProductsBronzeStage1Pipeline


class UpcListProductImportPipelineConfig(SingleSourceDataPipelineConfig):
    """Configuration class for all product import jobs that pull from the Altria UPC List."""

    __input_file_updated_at: Optional[datetime]

    def __init__(self, input_file_updated_at: Optional[datetime] = None, input_location: Optional[str] = None,
                 output_checkpoint_location: Optional[str] = None, output_database_location: Optional[str] = None,
                 output_hive_database_force_recreate=False, output_location: Optional[str] = None,
                 output_prefix: Optional[str] = None) -> None:
        """Initialize a new instance of the UpcListProductImportPipelineConfig.

        Parameters
        ----------
        input_file_updated_at: datetime
            The last update timestamp of the input file that is being used.

        input_location: str
            Path for the left input of the pipeline.

        output_checkpoint_location: str
            Path for the output of the pipeline's streaming checkpoints.

        output_database_location: str
            Path for the database that this pipline is a part of.

        output_hive_database_force_recreate: bool
            A flag indicating that the output hive database needs to be dropped and recreated.

        output_location: str
            Path for the output of the pipeline.

        output_prefix: str
            An optional prefix to be prepended to the pipeline's output path.
            Mainly used for blue-green deployment paths."""

        super().__init__(None, input_location, output_checkpoint_location, output_database_location,
                         output_hive_database_force_recreate, output_location, output_prefix)

        if not input_file_updated_at:
            raise ArgumentError("input_file_updated_at is required")

        self.__input_file_updated_at = input_file_updated_at

    @property
    def input_file_updated_at(self) -> Optional[datetime]:
        """Gets last update timestamp of the input file that is being used."""

        return self.__input_file_updated_at


class UpcListProductImportPipeline(ProductsBronzeStage1Pipeline, ABC):
    input_format = IoResourceFormats.EXCEL
    input_name = ""
    output_mode = FileIoOutputModes.APPEND

    def __init__(self, config: UpcListProductImportPipelineConfig) -> None:
        super().__init__(config)

    def before_save(self, src: DataFrame) -> DataFrame:
        return (super()
                .before_save(src)
                .withColumn("created_at", lit(self.config.input_file_updated_at))
                .withColumn("updated_at", lit(self.config.input_file_updated_at)))

    @property
    def config(self) -> UpcListProductImportPipelineConfig:
        return cast(UpcListProductImportPipelineConfig, super().config)


class UpcListCigaretteProductImportPipeline(UpcListProductImportPipeline):
    input_name = "cigarette-product-list"
    input_io_opts = {
        "sheet_name": "Cigarette Product List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = CIGARETTES_TAB_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListCigaretteProductMapper.map_to_products_bronze_stage_1(src)


class UpcListCigarProductImportPipeline(UpcListProductImportPipeline):
    input_name = "cigar-product-list"
    input_io_opts = {
        "sheet_name": "Cigar Product List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = CIGARS_TAB_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListCigarProductMapper.map_to_products_bronze_stage_1(src)


class UpcListSmokelessProductImportPipeline(UpcListProductImportPipeline):
    input_name = "smokeless-product-list"
    input_io_opts = {
        "sheet_name": "Smokeless Product List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = SMOKELESS_TAB_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListSmokelessProductMapper.map_to_products_bronze_stage_1(src)


class UpcListTDPProductImportPipeline(UpcListProductImportPipeline):
    input_name = "tdp-product-list"
    input_io_opts = {
        "sheet_name": "TDP Product List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = TDP_TAB_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListTDPProductMapper.map_to_products_bronze_stage_1(src)


class UpcListEVaporProductImportPipeline(UpcListProductImportPipeline):
    input_name = "evapor-product-list"
    input_io_opts = {
        "sheet_name": "EVapor Product List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = EVAPOR_TAB_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListEvaporProductMapper.map_to_products_bronze_stage_1(src)


class UpcListTobaccoAccessoriesProductImportPipeline(UpcListProductImportPipeline):
    input_name = "tobacco-accessories-product-list"
    input_io_opts = {
        "sheet_name": "Tobacco Accessories Prod List",
        "skiprows": [0, 1, 2],
        "usecols": "A:E",
        "dtype": "str"
    }
    input_schema = TOBACCO_ACCESSORIES_UPC_LIST_SCHEMA

    def mapper_function(self, src: DataFrame):
        return UpcListTobaccoAccessoriesProductMapper.map_to_products_bronze_stage_1(src)
