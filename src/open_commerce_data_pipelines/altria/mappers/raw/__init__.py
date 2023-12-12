from pyspark.sql import DataFrame
# pylint: disable=redefined-builtin
from pyspark.sql.functions import array, struct, col, lit, explode, split, transform, when, map_from_entries, \
    element_at, coalesce, concat_ws, filter, regexp_replace, lower, array_except, expr, map_filter, regexp_extract

from open_commerce_data_pipelines.altria.mappers.raw.constants.products import FLAVORS, FILTER_TYPES, TDP_PRODUCT_SIZES, \
    MANUFACTURER_PARENT_MAP, MANUFACTURER_BRAND_MAP, SMOKELESS_PRODUCT_SIZES, CIGARETTE_PRODUCT_SIZES, \
    CIGAR_PRODUCT_SIZES, PACK_TYPES, TEXT_TRANSFORM_MAP


class UpcListCigaretteProductMapper:
    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("pack_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("pack").alias("selling_unit_of_measure")),
                                  struct(col("carton_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("carton").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*CIGARETTE_PRODUCT_SIZES)),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("pack_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("pk")),
                                       1))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("pack_size")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor")) &
                                                        ~x.eqNullSafe(col("filter_type")))))
                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size", "ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigarettes")).alias("categories")))


class UpcListCigarProductMapper:
    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("pack_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("pack").alias("selling_unit_of_measure")),
                                  struct(col("tray_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("tray").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*CIGAR_PRODUCT_SIZES)),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("flavor_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       2))
                .withColumn("flavor_3",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       3))
                .withColumn("flavor",
                            concat_ws(" ",
                                      *[element_at(filter("facets",
                                                          lambda x: lower(x).isin(*FLAVORS)),
                                                   i) for i in range(1, 5)]))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("get_pack_sizes",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ct")),
                                       1))
                .withColumn("pack_sizes", split("get_pack_sizes", "/"))
                .withColumn("pack_size",
                            when(col("classification.selling_unit_of_measure") == "pack",
                                 element_at(filter("pack_sizes",
                                                   lambda x: lower(x).endswith("ct")),
                                            1))
                            .otherwise(element_at(filter("pack_sizes",
                                                         lambda x: ~lower(x).endswith("ct")),
                                                  1)))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("get_pack_sizes")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor_1")) &
                                                        ~x.eqNullSafe(col("flavor_2")) &
                                                        ~x.eqNullSafe(col("flavor_3")) &
                                                        ~x.eqNullSafe(col("filter_type")))))
                .withColumn("facets_after_transform_2", col("facets"))

                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size", "ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigars")).alias("categories")))


class UpcListSmokelessProductMapper:
    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("can_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("can").alias("selling_unit_of_measure")),
                                  struct(col("roll_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("roll").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*SMOKELESS_PRODUCT_SIZES)),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("flavor_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       2))
                .withColumn("flavor_3",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       3))
                .withColumn("flavor",
                            concat_ws(" ",
                                      *[element_at(filter("facets",
                                                          lambda x: lower(x).isin(*FLAVORS)),
                                                   i) for i in range(1, 5)]))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("get_pack_sizes",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ct")),
                                       1))
                .withColumn("pack_sizes", split("get_pack_sizes", "/"))
                .withColumn("pack_size",
                            when(col("classification.selling_unit_of_measure") == "roll",
                                 element_at(filter("pack_sizes",
                                                   lambda x: lower(x).endswith("ct")),
                                            1))
                            .otherwise(element_at(filter("pack_sizes",
                                                         lambda x: ~lower(x).endswith("ct")),
                                                  1)))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("get_pack_sizes")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor_1")) &
                                                        ~x.eqNullSafe(col("flavor_2")) &
                                                        ~x.eqNullSafe(col("flavor_3")) &
                                                        ~x.eqNullSafe(col("filter_type")))))
                .withColumn("facets_after_transform_2", col("facets"))

                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size", "ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigars")).alias("categories")))


class UpcListTDPProductMapper:

    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("can_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("can").alias("selling_unit_of_measure")),
                                  struct(col("carton_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("carton").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*TDP_PRODUCT_SIZES)),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("flavor_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       2))
                .withColumn("flavor_3",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       3))
                .withColumn("flavor",
                            concat_ws(" ",
                                      *[element_at(filter("facets",
                                                          lambda x: lower(x).isin(*FLAVORS)),
                                                   i) for i in range(1, 5)]))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("get_pack_sizes",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ct")),
                                       1))
                .withColumn("nicotine_amount_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("mg")),
                                       1))
                .withColumn("nicotine_amount_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("%")),
                                       1))
                .withColumn("nicotine_amount",
                            concat_ws(" ", col("nicotine_amount_1"), col("nicotine_amount_2")))
                .withColumn("pack_sizes", split("get_pack_sizes", "/"))
                .withColumn("pack_size",
                            when(col("classification.selling_unit_of_measure") == "carton",
                                 element_at(filter("pack_sizes",
                                                   lambda x: lower(x).endswith("ct")),
                                            1))
                            .otherwise(element_at(filter("pack_sizes",
                                                         lambda x: ~lower(x).endswith("ct")),
                                                  1)))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("get_pack_sizes")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor_1")) &
                                                        ~x.eqNullSafe(col("flavor_2")) &
                                                        ~x.eqNullSafe(col("flavor_3")) &
                                                        ~x.eqNullSafe(col("filter_type")) &
                                                        ~x.eqNullSafe(col("nicotine_amount_1")) &
                                                        ~x.eqNullSafe(col("nicotine_amount_2"))
                                                        )))
                .withColumn("facets_after_transform_2", col("facets"))

                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size", "ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")),
                                                          struct(lit("nicotine_amount"), lower("nicotine_amount")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigars")).alias("categories")))


class UpcListEvaporProductMapper:
    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("saleable_unit_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("saleable_unit").alias("selling_unit_of_measure")),
                                  struct(col("packing_unit_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("packing_unit").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ml")),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("flavor_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       2))
                .withColumn("flavor_3",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       3))
                .withColumn("flavor",
                            concat_ws(" ",
                                      *[element_at(filter("facets",
                                                          lambda x: lower(x).isin(*FLAVORS)),
                                                   i) for i in range(1, 5)]))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("get_pack_sizes_ct",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ct")),
                                       1))
                .withColumn("get_pack_sizes_pk",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("pk")),
                                       1))
                .withColumn("pack_sizes_ct", split("get_pack_sizes_ct", "/"))
                .withColumn("get_pack_sizes_pk",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("pk")),
                                       1))
                .withColumn("pack_sizes_pk",
                            regexp_extract(col("get_pack_sizes_pk"), r'(\d+)', 1))
                .withColumn("pack_size_ct",
                            when((col("classification.selling_unit_of_measure") == "packing_unit") &
                                 col("get_pack_sizes_ct").isNotNull(),
                                 element_at(filter("pack_sizes_ct",
                                                   lambda x: lower(x).endswith("ct")),
                                            1))
                            .when((col("classification.selling_unit_of_measure") == "packing_unit") &
                                  col("get_pack_sizes_pk").isNotNull(),
                                  col("pack_sizes_pk"))
                            .otherwise(element_at(filter("pack_sizes_ct",
                                                         lambda x: ~lower(x).endswith("ct")),
                                                  1)))
                .withColumn("nicotine_amount_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("mg")),
                                       1))
                .withColumn("nicotine_amount_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("%")),
                                       1))
                .withColumn("nicotine_amount",
                            concat_ws(" ", col("nicotine_amount_1"), col("nicotine_amount_2")))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("get_pack_sizes_ct")) &
                                                        ~x.eqNullSafe(col("get_pack_sizes_pk")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor_1")) &
                                                        ~x.eqNullSafe(col("flavor_2")) &
                                                        ~x.eqNullSafe(col("flavor_3")) &
                                                        ~x.eqNullSafe(col("filter_type")) &
                                                        ~x.eqNullSafe(col("nicotine_amount_1")) &
                                                        ~x.eqNullSafe(col("nicotine_amount_2")))))
                .withColumn("facets_after_transform_2", col("facets"))

                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size_ct", "cart|ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")),
                                                          struct(lit("nicotine_amount"), lower("nicotine_amount")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigars")).alias("categories")))


class UpcListTobaccoAccessoriesProductMapper:
    @staticmethod
    def map_to_products_bronze_stage_1(src: DataFrame):
        manufacturer_brand_map = map_from_entries(array(*[struct(lit(k),
                                                                 array(*[lit(x) for x in v]))
                                                          for k, v in MANUFACTURER_BRAND_MAP.items()]))
        manufacturer_parent_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                           for k, v in MANUFACTURER_PARENT_MAP.items()]))
        text_transform_map = map_from_entries(array(*[struct(lit(k), lit(v))
                                                      for k, v in TEXT_TRANSFORM_MAP.items()]))

        return (src
                .withColumn("upcs",
                            array(struct(col("saleable_unit_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("saleable_unit").alias("selling_unit_of_measure")),
                                  struct(col("packing_unit_upc").alias("upc"),
                                         lit(1.0).alias("selling_unit"),
                                         lit("packing_unit").alias("selling_unit_of_measure"))))
                .withColumn("classification", explode("upcs"))
                .withColumn("upc", col("classification").upc)
                .withColumn("selling_unit", col("classification").selling_unit)
                .withColumn("selling_unit_of_measure", col("classification").selling_unit_of_measure)
                .where(~col("upc").startswith("NNN"))
                .withColumn("facets", split("description", " "))
                .withColumn("facets",
                            transform("facets",
                                      lambda x: coalesce(element_at(text_transform_map, x), x)))
                .withColumn("cleaned", concat_ws(" ", "facets"))
                .withColumn("brand",
                            element_at(filter(element_at(manufacturer_brand_map, col("manufacturer")),
                                              lambda x: col("cleaned").contains(x)),
                                       1))
                .withColumn("facets",
                            when(col("brand").isNotNull(),
                                 array_except("facets", split("brand", " ")))
                            .otherwise(col("facets")))
                .withColumn("product_size",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ml")),
                                       1))
                .withColumn("pack_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*PACK_TYPES)),
                                       1))
                .withColumn("flavor_1",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       1))
                .withColumn("flavor_2",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       2))
                .withColumn("flavor_3",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FLAVORS)),
                                       3))
                .withColumn("flavor",
                            concat_ws(" ",
                                      *[element_at(filter("facets",
                                                          lambda x: lower(x).isin(*FLAVORS)),
                                                   i) for i in range(1, 5)]))
                .withColumn("filter_type",
                            element_at(filter("facets",
                                              lambda x: lower(x).isin(*FILTER_TYPES)),
                                       1))
                .withColumn("get_pack_sizes_ct",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("ct")),
                                       1))
                .withColumn("get_pack_sizes_pk",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("pk")),
                                       1))
                .withColumn("pack_sizes_ct", split("get_pack_sizes_ct", "/"))
                .withColumn("get_pack_sizes_pk",
                            element_at(filter("facets",
                                              lambda x: lower(x).endswith("pk")),
                                       1))
                .withColumn("pack_sizes_pk",
                            regexp_extract(col("get_pack_sizes_pk"), r'(\d+)', 1))
                .withColumn("pack_size_ct",
                            when((col("classification.selling_unit_of_measure") == "packing_unit") &
                                 col("get_pack_sizes_ct").isNotNull(),
                                 element_at(filter("pack_sizes_ct",
                                                   lambda x: lower(x).endswith("ct")),
                                            1))
                            .when((col("classification.selling_unit_of_measure") == "packing_unit") &
                                  col("get_pack_sizes_pk").isNotNull(),
                                  col("pack_sizes_pk"))
                            .otherwise(element_at(filter("pack_sizes_ct",
                                                         lambda x: ~lower(x).endswith("ct")),
                                                  1)))
                .withColumn("facets", filter("facets",
                                             lambda x: (~x.rlike(r"\(.+\)") &
                                                        ~x.eqNullSafe(col("get_pack_sizes_ct")) &
                                                        ~x.eqNullSafe(col("get_pack_sizes_pk")) &
                                                        ~x.eqNullSafe(col("pack_type")) &
                                                        ~x.eqNullSafe(col("product_size")) &
                                                        ~x.eqNullSafe(col("flavor_1")) &
                                                        ~x.eqNullSafe(col("flavor_2")) &
                                                        ~x.eqNullSafe(col("flavor_3")) &
                                                        ~x.eqNullSafe(col("filter_type")))))
                .withColumn("facets_after_transform_2", col("facets"))

                .select(expr("uuid()").alias("id"),
                        "manufacturer",
                        element_at(manufacturer_parent_map, col("manufacturer"))
                        .alias("manufacturer_parent_company"),
                        "brand",
                        concat_ws(" ", "facets").alias("product_line"),
                        "upc",
                        "selling_unit",
                        "selling_unit_of_measure",
                        lit(None).cast("string").alias("display_name_template"),
                        lit(None).cast("string").alias("sample_image_url"),
                        map_filter(map_from_entries(array(struct(lit("filter_type"), lower("filter_type")),
                                                          struct(lit("flavor"), lower("flavor")),
                                                          struct(lit("pack_size"),
                                                                 regexp_replace("pack_size_ct", "cart|ct|pk", "")),
                                                          struct(lit("pack_type"), lower("pack_type")),
                                                          struct(lit("product_size"), lower("product_size")))),
                                   lambda _, v: v.isNotNull())
                        .alias("attributes"),
                        array(lit("tobacco"), lit("cigars")).alias("categories")))
