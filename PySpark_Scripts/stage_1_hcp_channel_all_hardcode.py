import json
import logging
import configparser
import boto3
import sys
import time
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_config_file(config_file_name):
    """
    This function is reading config file and returning config object
    :type config_file_name: string
    """
    config = configparser.ConfigParser()
    config.read(config_file_name)
    return config


def check_config_section(config, env):
    """
    This function check if section specified in job parameters is present in config file or not
    :param config: Config file object
    :param env: environment which is specified in job parameters
    """
    if not config.has_section(env):
        raise Exception("Environment details doesn't exist in configuration file")


class NvsETL:
    def __init__(self, config, args, glue_context, spark):
        """
        This is init method used to initialize variables in class
        :param config: Config file object
        :param args: Arguments given to Glue job
        """
        self.env = args['environment']
        self.args = args
        self.bucket_name = config.get(self.env, "bucket_name")
        self.normalized_folder = json.loads(config.get(self.env, "folder_names"))['normalized']
        self.s3_path_list = []
        self.normalized_database = json.loads(config.get(self.env, "athena_database"))['normalized']
        self.landing_database = json.loads(config.get(self.env, "athena_database"))['landing']
        self.summarized_database = json.loads(config.get(self.env, "athena_database"))['summarized']
        self.config = config
        self.crawler_name = 'mars_nvs_crawler'
        self.glue_context = glue_context
        self.spark = spark
        self.mstr_cm_mdm_prfl_wkl_tbl = config.get(self.env, "mstr_cm_mdm_prfl_wkl_tbl")
        self.mars_population_demographics_staging = config.get(self.env, "population_demo_tbl")
        self.hcp_org_map_tbl = config.get(self.env, "hcp_org_map_tbl")
        self.nvs_calls_01232024 = config.get(self.env, "nvs_calls_01232024_staging")
        self.nvs_calls_06252024 = config.get(self.env, "nvs_calls_06252024_staging")
        self.nvs_calls_09252024 = config.get(self.env, "nvs_calls_09252024_staging")
        self.nvs_dtc_display = config.get(self.env, "nvs_display_staging")
        self.nvs_dtc_display_09252024 = config.get(self.env, "nvs_display_09252024_staging")
        self.nvs_costs = config.get(self.env, "nvs_costs_staging")
        self.nvs_dtc_search_01232024 = config.get(self.env, "nvs_dtc_search_01232024_staging")
        self.nvs_dtc_search_09252024 = config.get(self.env, "nvs_dtc_search_09252024_staging")
        self.nvs_poc = config.get(self.env, "nvs_poc_staging")
        self.nvs_poc_09252024 = config.get(self.env, "nvs_poc_09252024_staging")
        self.nvs_social = config.get(self.env, "nvs_social_staging")
        self.nvs_social_09252024 = config.get(self.env, "nvs_social_09252024_staging")
        self.nvs_hcp_search_01232024 = config.get(self.env, "nvs_hcp_search_01232024_staging")
        self.nvs_hcp_search_10032024 = config.get(self.env, "nvs_hcp_search_10032024_staging")
        self.nvs_hcp_all = config.get(self.env, "nvs_hcp_all_staging")
        self.nvs_hcp_all_12022024 = config.get(self.env, "nvs_hcp_all_12022024_staging")
        self.nvs_calls_03052025_staging = config.get(self.env, "nvs_calls_03052025_staging")
        self.nvs_dtc_display_03072025_staging = config.get(self.env, "nvs_dtc_display_03072025_staging")
        self.nvs_dtc_search_03072025_staging = config.get(self.env, "nvs_dtc_search_03072025_staging")
        self.nvs_dtc_poc_03072025_staging = config.get(self.env, "nvs_dtc_poc_03072025_staging")
        self.nvs_hcp_search_03072025_staging = config.get(self.env, "nvs_hcp_search_03072025_staging")
        self.nvs_hcp_search_10032024_staging = config.get(self.env, "nvs_hcp_search_10032024_staging")
        self.nvs_hcp_all_03072025_staging = config.get(self.env, "nvs_hcp_all_03072025_staging")
        self.nvs_hcp_poc_03072025_staging = config.get(self.env, "nvs_hcp_poc_03072025_staging")
        self.nvs_hcp_social_03072025_staging = config.get(self.env, "nvs_hcp_social_03072025_staging")
        self.nvs_costs_unpivot_03182025_staging = config.get(self.env, "nvs_costs_unpivot_03182025_staging")
        self.nvs_calls_03262025_staging = config.get(self.env, "nvs_calls_03262025_staging")

    def nvs_tam(self, athena_client):
        df_tam_ce = self.spark.sql(f"""
                    with raw_calls_unioned as(
                        select 
                            npi_num, zip_cd, city, state, brand, yrmo, call_p1, call_p2, call_p3, calls, lunch_n_learn_calls 
                        from {self.normalized_database}.{self.nvs_calls_01232024}
                        where yrmo between 202201 and 202206
                        union all 
                        select 
                            npi_num, zip_cd, city, state, brand, yrmo, call_p1, call_p2, call_p3, calls, lunch_n_learn_calls 
                        from {self.normalized_database}.{self.nvs_calls_06252024}  
                        where yrmo between 202207 and 202212
                        union all 
                        select 
                            npi_num, zip_cd, city, state, brand, yrmo, call_p1, call_p2, call_p3, calls, lunch_n_learn_calls
                        from {self.normalized_database}.{self.nvs_calls_09252024}
                        where yrmo between 202301 and 202312
                        union all 
                        select 
                            npi_num, zip_cd, city, state, brand, yrmo, call_p1, call_p2, call_p3, calls, lunch_n_learn_calls 
                        from {self.normalized_database}.{self.nvs_calls_03262025_staging}
                        where yrmo >= 202401
                    ),
                    normalized as(
                            select 
                                   hcp.mdm_zip as zip_code,
                                   nvs.yrmo as year_month,
                                   'XOLAIR' as product_brand_name,
                                    case when call_p1 = '1' or lunch_n_learn_calls = '1' then 1 
                                        when call_p2 = '1' then 2
                                        when call_p3 = '1' then 3 
                                   end as display_order
                            from raw_calls_unioned as nvs
                            inner join 
                                  {self.normalized_database}.{self.mstr_cm_mdm_prfl_wkl_tbl} as mdm
                            on nvs.npi_num = mdm.npi_number 
                            inner join 
                                  {self.normalized_database}.{self.hcp_org_map_tbl} as hcp 
                            on mdm.mdm_id = hcp.mdm_id and array_contains(hcp.product_brand_name, 'XOLAIR')
                            where nvs.npi_num is not null
                      ),
                        tam_hd_costs as(
                           select 'XOLAIR' as brand, 32000000.0 as cost, '2022' as year
                           union all
                           select 'XOLAIR' as brand, 32000000.0 as cost, '2023' as year
                           union all 
                           select 'XOLAIR' as brand, 36583323.0 as cost, '2024' as year
                      ),
                        hcp_costs as(
                            select 
                                n.product_brand_name, 
                                n.year_month, 
                                n.zip_code as zip,
                                'CE' as audience, 
                                'tam_hd' as channel, 
                                1.0 / n.display_order as reach, 
                                c.cost
                            from normalized as n 
                            inner join tam_hd_costs as c 
                            on n.product_brand_name = c.brand and substring(n.year_month, 1, 4) = c.year 
                       ),
                        final_agg as(
                           select 
                                product_brand_name, 
                                'NVS' as source,
                                year_month, 
                                zip,
                                audience,
                                channel, 
                                reach,
                                cast(null as double) as engage,
                                cost * reach / sum(reach) over(partition by product_brand_name, substring(year_month, 1, 4)) as cost
                          from hcp_costs 
                        ),
                        final_agg_casted as(
                            select 
                                product_brand_name, 
                                source, 
                                year_month, 
                                cast(zip as string) as zip, 
                                audience, 
                                channel,
                                sum(cast(reach as double)) as reach, 
                                sum(cast(engage as double)) as engage, 
                                sum(cast(cost as double)) as cost 
                            from final_agg
                            group by 1, 2, 3, 4, 5, 6
                        )
                        select * from final_agg_casted
                """)
        df_tam_ce.registerTempTable('tam_ce')

        tam_df_persisted = df_tam_ce.persist()
        audit_info = []
        audit_info += [{'table_name': f"{self.normalized_database}.mars_tam_nvs_staging",
                        'rows_updated': tam_df_persisted.count()}]

        s3_path = ("s3://" + self.bucket_name + "/" + self.normalized_folder + "/etl/nvs/mars_tam_nvs_staging/")
        tam_df_persisted.coalesce(8).write.mode('overwrite').parquet(s3_path)
        self.s3_path_list += [s3_path]

        # Checking historical tables in normalized database
        paginator = athena_client.get_paginator('list_table_metadata')
        response_iterator = paginator.paginate(CatalogName='AwsDataCatalog', DatabaseName=self.normalized_database)

        all_table_names = []
        for page in response_iterator:
            all_table_names.extend((i['Name'] for i in page['TableMetadataList']))

        # Creating list of backup tables in normalized database
        existing_historical_table_names = []
        for tbl_name in all_table_names:
            if 'historical' in tbl_name:
                existing_historical_table_names.append(tbl_name)

        if 'mars_tam_nvs_historical' in existing_historical_table_names:
            historical_up_df = self.glue_context.create_data_frame.from_catalog(database=self.normalized_database,
                                                                                table_name='mars_tam_nvs_historical')
            last_version = historical_up_df.select([max("version")]).head()[0]
            hist_df = tam_df_persisted.withColumn('version', lit(int(last_version) + 1))
        else:  # This means table is new to landing, it is getting loaded into backup for first time
            hist_df = tam_df_persisted.withColumn('version', lit(1))
            logger.info(f"Back up table does not exist for mars_tam_nvs_historical in normalized layer")

        audit_info.append(
            {'table_name': f"{self.normalized_database}.mars_tam_nvs_historical",
             'rows_updated': tam_df_persisted.count()})

        s3_path = ("s3://" + self.bucket_name + "/" + self.normalized_folder + "/etl/nvs/mars_tam_nvs_historical/")
        hist_df.coalesce(8).write.partitionBy("version").mode('append').parquet(s3_path)
        self.s3_path_list += [s3_path]

        return audit_info

    def nvs_digital(self, athena_client):
        df_dtc_display = self.spark.sql(f"""
            with display_unioned as(
                select 
                    year_mth, dma_region, dma_code, impressions, clicks
                from {self.normalized_database}.{self.nvs_dtc_display} 
                where year_mth between 202201 and 202212
                union all 
                select 
                    year_mth, dma_region, dma_code, impressions, clicks
                from {self.normalized_database}.{self.nvs_dtc_display_09252024} 
                where year_mth between 202301 and 202312
                union all
                select 
                    year_mth, dma_region, dma_code, impressions, clicks
                from {self.normalized_database}.{self.nvs_dtc_display_03072025_staging} where year_mth >= 202401
            ),
            cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
            ), 
            nvs_cleaned as(
                select 
                    'XOLAIR' as brand, 
                    'Display' as channel, 
                    'DTC' as audience, 
                    substring(year_mth, 1, 4) as year,
                    substring(year_mth, 5, 2) as month,
                    dma_code,
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(cast(impressions as double)) as reach, 
                    sum(cast(clicks as double)) as engage
                from display_unioned as nvs
                group by 1, 2, 3, 4, 5, 6
            ), 
            dma_joined as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    month,
                    year,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from nvs_cleaned as nvs 
                join cleaned_dmas as cd 
                on nvs.dma_code = cd.dma_code 
            ),
            costs_cleaned as(
                select 
                    substring(date_month_, 1, 4) AS year,
                    substring(date_month_, 6, 2) AS month,
                    cast(replace(dtc_display_, ',', '') as double) as cost
                from {self.normalized_database}.{self.nvs_costs}
                where date_month_ <= '2023-12'
                union all
                select 
                    substring(year_month, 1, 4) as year,
                    substring(year_month, 5, 2) as month,
                    cost
                from {self.normalized_database}.{self.nvs_costs_unpivot_03182025_staging}
                where audience = 'DTC' and upper(channel) = 'DIGITAL DISPLAY'
            ),
            final_agg as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    dj.year,
                    cast(dj.month as integer) as month,
                    zip_code, 
                    dma,
                    state, 
                    country,
                    reach,
                    engage,
                    cast(cc.cost as double) * reach / sum(reach) over(partition by brand, dj.month, dj.year) as cost
                from dma_joined dj 
                inner join costs_cleaned cc 
                on dj.year = cc.year and dj.month = cc.month 
            )
            select 
                *
            from final_agg 
                                                                
        """)
        df_dtc_display.registerTempTable('dtc_display')

        df_dtc_paid_search = self.spark.sql(f"""                           
            with search_unioned as(
                select 
                    year_mth, dma_name, dma_code, impressions, clicks                            
                from {self.normalized_database}.{self.nvs_dtc_search_01232024}  
                where year_mth between 202201 and 202212
                union all 
                select 
                    year_mth, dma_name, dma_code, impressions, clicks  
                from {self.normalized_database}.{self.nvs_dtc_search_09252024}
                where year_mth between 202301 and 202312
                union all 
                select 
                      year_mth, dma_name, dma_code, impressions, clicks 
                from {self.normalized_database}.{self.nvs_dtc_search_03072025_staging}
                where year_mth >= 202401             
            ),
            cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
            ),
            nvs_cleaned as(
                select 
                    'XOLAIR' as brand, 
                    'Paid Search' as channel, 
                    'DTC' as audience, 
                    substring(year_mth, 1, 4) as year,
                    substring(year_mth, 5, 2) as month,
                    dma_code,
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(cast(impressions as double)) as reach, 
                    sum(cast(clicks as double)) as engage
                from search_unioned as nvs
                group by 1, 2, 3, 4, 5, 6
            ),
            dma_joined as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    month,
                    year,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from nvs_cleaned as nvs 
                join cleaned_dmas as cd 
                on nvs.dma_code = cd.dma_code 
            ),
            costs_cleaned as(
                select 
                    substring(date_month_, 1, 4) AS year,
                    substring(date_month_, 6, 2) AS month,
                    cast(replace(dtc_search, ',', '') as double) as cost
                from {self.normalized_database}.{self.nvs_costs}
                where date_month_ <= '2023-12'
                union all 
                select 
                    substring(year_month, 1, 4) as year,
                    substring(year_month, 5, 2) as month,
                    cost
                from {self.normalized_database}.{self.nvs_costs_unpivot_03182025_staging}
                where audience = 'DTC' and upper(channel) = 'PAID SEARCH'
            ),
            final_agg as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    dj.year,
                    cast(dj.month as integer) as month,
                    zip_code, 
                    dma,
                    state, 
                    country,
                    reach,
                    engage,
                    cast(cc.cost as double) * reach / sum(reach) over(partition by brand, dj.month, dj.year) as cost
                from dma_joined dj 
                inner join costs_cleaned cc 
                on dj.year = cc.year and dj.month = cc.month 
            )
            select 
                *
            from final_agg
        """)
        df_dtc_paid_search.registerTempTable('dtc_paid_search')


        df_dtc_poc = self.spark.sql(f"""
            with poc_unioned as(
                select year_mth, dma, dma_code, impressions from {self.normalized_database}.{self.nvs_poc}
                where year_mth between 202201 and 202212
                union all
                select year_mth, dma, dma_code, impressions from {self.normalized_database}.{self.nvs_poc_09252024}
                where year_mth between 202301 and 202312
                union all 
                select year_mth, dma, dma_code, impressions from {self.normalized_database}.{self.nvs_dtc_poc_03072025_staging}
                where year_mth >= 202401
            ),                
            cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
            ),
            nvs_cleaned as(
                select 
                    'XOLAIR' as brand, 
                    'Point of Care' as channel, 
                    'DTC' as audience, 
                    substring(year_mth, 1, 4) as year,
                    substring(year_mth, 5, 2) as month,
                    dma_code,
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(cast(impressions as double)) as reach, 
                    cast(null as double) as engage
                from poc_unioned as nvs
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9
            ),
            dma_joined as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    month,
                    year,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from nvs_cleaned as nvs 
                join cleaned_dmas as cd 
                on nvs.dma_code = cd.dma_code 
            ),
            costs_cleaned as(
                select 
                    substring(date_month_, 1, 4) AS year,
                    substring(date_month_, 6, 2) AS month,
                    cast(replace(dtc_poc, ',', '') as double) as cost
                from {self.normalized_database}.{self.nvs_costs}
                where date_month_ <= '2023-12'
            ),
            final_agg as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    dj.year,
                    cast(dj.month as integer) as month,
                    zip_code, 
                    dma,
                    state, 
                    country,
                    reach,
                    engage,
                    cast(cc.cost as double) * reach / sum(reach) over(partition by brand, dj.month, dj.year) as cost
                from dma_joined dj 
                left join costs_cleaned cc 
                on dj.year = cc.year and dj.month = cc.month 
            )
            select 
                *
            from final_agg
        """)
        df_dtc_poc.registerTempTable('dtc_poc')


        df_dtc_paid_social = self.spark.sql(f"""
            with paid_social_unioned as(
                select dma_code, dma_name, year_mth, impressions, clicks from {self.normalized_database}.{self.nvs_social}
                where year_mth between 202201 and 202212
                union all
                select dma_code, dma_name, year_mth, impressions, clicks from {self.normalized_database}.{self.nvs_social_09252024}
                where year_mth >= 202301
            ),
            cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
            ),
            nvs_cleaned as(
                select 
                    'XOLAIR' as brand, 
                    'Paid Social' as channel, 
                    'DTC' as audience, 
                    substring(year_mth, 1, 4) as year,
                    substring(year_mth, 5, 2) as month,
                    dma_code,
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(cast(impressions as double)) as reach, 
                    sum(cast(clicks as double)) as engage
                from paid_social_unioned as nvs
                group by 1, 2, 3, 4, 5, 6
            ),
            dma_joined as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    month,
                    year,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from nvs_cleaned as nvs 
                join cleaned_dmas as cd 
                on nvs.dma_code = cd.dma_code 
            ),

            costs_cleaned as(
                select 
                    substring(date_month_, 1, 4) AS year,
                    substring(date_month_, 6, 2) AS month,
                    cast(replace(dtc_social, ',', '') as double) as cost
                from {self.normalized_database}.{self.nvs_costs}
            ),
            final_agg as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    dj.year,
                    cast(dj.month as integer) as month,
                    zip_code, 
                    dma,
                    state, 
                    country,
                    reach,
                    engage,
                    cast(cc.cost as double) * reach / sum(reach) over(partition by brand, dj.month, dj.year) as cost
                from dma_joined dj 
                left join costs_cleaned cc 
                on dj.year = cc.year and dj.month = cc.month 
            )
            select *
            from final_agg 
        """)
        df_dtc_paid_social.registerTempTable('dtc_paid_social')

        df_paid_search_hcp = self.spark.sql(f"""                         
            with paid_search_unioned as(
                select dma_code, date_format(cast(activity_date as date), 'yyyyMM') as year_mth, impressions, clicks 
                from {self.normalized_database}.{self.nvs_hcp_search_01232024}
                where activity_date <= '2022-12-31'
                union all 
                select dma_code, year_mth, impressions, clicks 
                from {self.normalized_database}.{self.nvs_hcp_search_10032024_staging}
                where year_mth between 202301 and 202312
                union all 
                select dma_code, year_mth, impressions, clicks 
                from {self.normalized_database}.{self.nvs_hcp_search_03072025_staging}
                where year_mth >= 202401                             
            ),
            cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
            ),
            nvs_cleaned as (
                select 
                    'XOLAIR' as brand, 
                    'Paid Search' as channel, 
                    'HCP' as audience, 
                    substring(year_mth, 1, 4) as year,
                    substring(year_mth, 5, 2) as month,
                    dma_code,
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(cast(impressions as double)) as reach, 
                    sum(cast(clicks as double)) as engage
                from paid_search_unioned as nvs
                group by 1, 2, 3, 4, 5, 6
            ),
            dma_joined as(
                select 
                    brand, 
                    channel, 
                    audience, 
                    month,
                    year,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from nvs_cleaned as nvs 
                join cleaned_dmas as cd 
                on nvs.dma_code = cd.dma_code 
            ),
            nvs_costs_paid_search as (
                select 
                    substring(date_month_, 1, 4) AS year,
                    substring(date_month_, 6, 2) AS month,
                    cast(replace(npp, ',', '') as double) * 0.16 as paid_search_cost
                from {self.normalized_database}.{self.nvs_costs}
                where date_month_ <= '2023-12'
                union all
                select 
                    substring(year_month, 1, 4) as year,
                    substring(year_month, 5, 2) as month,
                    sum(cost) as paid_search_cost
                from {self.normalized_database}.{self.nvs_costs_unpivot_03182025_staging}
                where audience = 'HCP'
                  and upper(channel) = 'PAID SEARCH'
                group by 1, 2
            ),
            monthly_reach as (
            select 
                year,
                month,
                sum(reach) as total_monthly_reach
            from dma_joined
            group by year, month
            ),
          final_allocation as (
            select 
                dj.brand,
                dj.channel,
                dj.audience,
                dj.year,
                cast(dj.month as integer) as month,
                dj.zip_code,
                dj.dma,
                dj.state,
                dj.country,
                dj.reach,
                dj.engage,
                (dj.reach / mr.total_monthly_reach) * ca.paid_search_cost as cost
            from dma_joined dj
            join monthly_reach mr on dj.year = mr.year and dj.month = mr.month
            join nvs_costs_paid_search ca on dj.year = ca.year and dj.month = ca.month
        )
        select * from final_allocation
        """
        )
        df_paid_search_hcp.registerTempTable('hcp_paid_search')

        # df_hcp_channels_old = self.spark.sql(f"""
        #     with zip_data as(
        #             select 
        #                 distinct zip 
        #             from {self.summarized_database}.{self.mars_population_demographics_staging}
        #         ),
        #         pivoted_hcp_all as (
        #             select distinct 
        #                 'XOLAIR' as brand,
        #                 channel, 
        #                 'HCP' as audience,
        #                 cast(substring(yrwk, 1, 4) as string) as year,
        #                 cast(substring(yrwk, 5, 2) as string) as week,
        #                 zip_cd as zip_code,
        #                 cast(null as string) as dma,
        #                 cast(null as string) as state,
        #                 'US' as country,
        #                 sum(case when metric = 'REACH' then cast(value as double) end) as reach,
        #                 sum(case when metric = 'ENGAGEMENT' then cast(value as double) end) as engage
        #             from {self.normalized_database}.mars_nvs_hcp_all_staging as h 
        #             where zip_cd is not null and yrwk <= 202352
        #             group by 1, 2, 3, 4, 5, 6, 7, 8, 9
        #         ), 
        #         weekly_to_monthly as (
        #             select 
        #                 brand, 
        #                 case
        #                     when channel like 'EHR' then 'EHR/EMR'
        #                     when channel like '3RD_PARTY_EMAIL' then 'Third-Party Email'
        #                     when channel like 'POC' then 'Point of Care'
        #                     when channel like 'DISPLAY' then 'Display'
        #                     when channel in ('VIDEO', 'CUSTOM') then 'Custom'
        #                     when channel like 'ENDEMIC_SOCIAL' then 'Paid Social'
        #                 end as channel,
        #                 audience, 
        #                 year,
        #                 cast(date_format(add_months(to_date(concat(year, '-01-01'), 'yyyy-MM-dd'), (cast(week as int) - 1) / 4), 'MM') as int) as month,
        #                 zip_code, 
        #                 dma, 
        #                 state, 
        #                 country, 
        #                 reach, 
        #                 engage
        #             from pivoted_hcp_all p 
        #         ),
        #         deduped_data as (
        #             select distinct
        #                 brand,
        #                 channel,
        #                 audience,
        #                 year,
        #                 month,
        #                 cast(zip_code as string) as zip_code,
        #                 dma,
        #                 state,
        #                 country,
        #                 sum(reach) as reach,
        #                 sum(engage) as engage
        #             from weekly_to_monthly
        #             group by 1, 2, 3, 4, 5, 6, 7, 8, 9
        #         )
        #         select * from deduped_data
        # """)
        # df_hcp_channels_old.registerTempTable('hcp_channels_old')
        

        # df_hcp_channels_new = self.spark.sql(f"""
        #     with cleaned_dmas as(
        #         select 
        #             distinct 
        #             dma_code, 
        #             dma_name from {self.summarized_database}.{self.mars_population_demographics_staging}
        #     ),
        #     hcp_channels_combined as(
        #         select 
        #             ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, sum(clicks) as clicks
        #         from {self.normalized_database}.{self.nvs_hcp_all_03072025_staging}
        #         where year_mth >= 202401
        #         group by 1, 2, 3
        #         union all
        #         select 
        #             'POC' as ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, null as clicks
        #         from {self.normalized_database}.{self.nvs_hcp_poc_03072025_staging}
        #         where year_mth >= 202401
        #         group by 1, 2, 3
        #         union all 
        #         select 
        #             'Endemic Social' as ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, sum(clicks) as clicks
        #         from {self.normalized_database}.{self.nvs_hcp_social_03072025_staging}
        #         where year_mth >= 202401
        #         group by 1, 2, 3
        #     ),   
        #     hcp_raw_data as(
        #         select 
        #             'XOLAIR' as brand, 
        #             ipmm_channel as channel, 
        #             'HCP' as audience,
        #             dma_code,
        #             substring(year_mth, 1, 4) as year, 
        #             substring(year_mth, 5, 2) as month, 
        #             cast(null as string) as zip_code, 
        #             cast(null as string) as state, 
        #             'US' as country, 
        #             sum(impressions) as reach, 
        #             sum(clicks) as engage
        #         from hcp_channels_combined
        #         group by 1, 2, 3, 4, 5, 6, 7, 8, 9
        #     ), 
        #     dma_joined as(
        #         select 
        #             brand, 
        #             case
        #                 when channel like 'EHR' then 'EHR/EMR'
        #                 when channel like '3rd Party Email' then 'Third-Party Email'
        #                 when channel like 'POC' then 'Point of Care'
        #                 when channel like 'Digital Display' then 'Display'
        #                 when channel in ('Video', 'Custom') then 'Custom'
        #                 when channel like 'Endemic Social' then 'Paid Social'
        #             end as channel,
        #             audience, 
        #             year,
        #             cast(month as int) as month,
        #             zip_code, 
        #             cd.dma_name as dma,
        #             state, 
        #             country,
        #             reach,
        #             engage
        #         from hcp_raw_data as nvs 
        #         join cleaned_dmas as cd 
        #         on nvs.dma_code = cd.dma_code 
        #     )
        #     select * from dma_joined
        # """)
        # df_hcp_channels_new.registerTempTable('hcp_channels_new')

        df_hcp_all_cost_allocation = self.spark.sql(f"""
            with zip_data as(
                    select 
                        distinct zip 
                    from oasis_summarized_mars.mars_population_demographics
                ),
                pivoted_hcp_all as (
                    select distinct 
                        'XOLAIR' as brand,
                        channel, 
                        'HCP' as audience,
                        cast(substring(cast(yrwk as string), 1, 4) as string) as year,
                        cast(substring(cast(yrwk as string), 5, 2) as string) as week,
                        zip_cd as zip_code,
                        cast(null as string) as dma,
                        cast(null as string) as state,
                        'US' as country,
                        sum(case when metric = 'REACH' then cast(value as double) end) as reach,
                        sum(case when metric = 'ENGAGEMENT' then cast(value as double) end) as engage
                    from oasis_normalized_mars.mars_nvs_hcp_all_staging as h 
                    where zip_cd is not null and yrwk <= 202352
                    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
                ), 
                weekly_to_monthly as (
                    select 
                        brand, 
                        case
                            when channel like 'EHR' then 'EHR/EMR'
                            when channel like '3RD_PARTY_EMAIL' then 'Third-Party Email'
                            when channel like 'POC' then 'Point of Care'
                            when channel like 'DISPLAY' then 'Display'
                            when channel in ('VIDEO', 'CUSTOM') then 'Custom'
                            when channel like 'ENDEMIC_SOCIAL' then 'Paid Social'
                        end as channel,
                        audience, 
                        year,
                        CAST(date_format(date_add('month', CAST((CAST(week AS int) - 1) / 4 AS int), date_parse(concat(year, '-01-01'), '%Y-%m-%d')),'%m') AS int) AS month,
                        zip_code, 
                        dma, 
                        state, 
                        country, 
                        reach, 
                        engage
                    from pivoted_hcp_all p 
                ),
                hcp_channels_old as (
                    select distinct
                        brand,
                        channel,
                        audience,
                        year,
                        month,
                        cast(zip_code as string) as zip_code,
                        dma,
                        state,
                        country,
                        sum(reach) as reach,
                        sum(engage) as engage
                    from weekly_to_monthly
                    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
                ),
           cleaned_dmas as(
                select 
                    distinct 
                    dma_code, 
                    dma_name from oasis_summarized_mars.mars_population_demographics
            ),
            hcp_channels_combined as(
                select 
                   ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, sum(clicks) as clicks
                from oasis_normalized_mars.mars_nvs_hcp_all_03072025_staging
                where year_mth >= 202401
                group by 1, 2, 3
                union all
                select 
                    'POC' as ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, null as clicks
                from oasis_normalized_mars.mars_nvs_hcp_poc_03072025_staging
                where year_mth >= 202401
                group by 1, 2, 3
                union all 
                select 
                    'Endemic Social' as ipmm_channel, dma_code, year_mth, sum(impressions) as impressions, sum(clicks) as clicks
                from oasis_normalized_mars.mars_nvs_hcp_social_03072025_staging
                where year_mth >= 202401
                group by 1, 2, 3
            ),   
            hcp_raw_data as(
                select 
                    'XOLAIR' as brand, 
                    ipmm_channel as channel, 
                    'HCP' as audience,
                    dma_code,
                    substring(cast(year_mth as string), 1, 4) as year, 
                    substring(cast(year_mth as string), 5, 2) as month, 
                    cast(null as string) as zip_code, 
                    cast(null as string) as state, 
                    'US' as country, 
                    sum(impressions) as reach, 
                    sum(clicks) as engage
                from hcp_channels_combined
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9
            ), 
            hcp_channels_new as(
                select 
                    brand, 
                    case
                        when channel like 'EHR' then 'EHR/EMR'
                        when channel like '3rd Party Email' then 'Third-Party Email'
                        when channel like 'POC' then 'Point of Care'
                        when channel like 'Digital Display' then 'Display'
                        when channel in ('Video', 'Custom') then 'Custom'
                        when channel like 'Endemic Social' then 'Paid Social'
                    end as channel,
                    audience, 
                    year,
                    cast(month as string) as month,
                    zip_code, 
                    cd.dma_name as dma,
                    state, 
                    country,
                    reach,
                    engage
                from hcp_raw_data as nvs 
                join cleaned_dmas as cd 
                on cast(nvs.dma_code as string) = cd.dma_code 
            ),
             all_hcp_unioned as(
                select * from hcp_channels_old
                union all 
                select * from hcp_channels_new
            ) ,
 
            spend_data as(
                select 
                    substring(cast(date_month_ as string), 1, 4) as year, 
                    substring(cast(date_month_ as string), 6, 2) as month, 
                    cast(replace(cast(npp as string), ',', '') as double) as total_cost
                from oasis_normalized_mars.mars_nvs_costs_staging
                where date_month_ <= '2023-12'
            ), 
            channel_allocations_old as (
                 select 'XOLAIR' as brand, year, month, 'Display' as channel, total_cost * 0.39 as cost from spend_data
                 union all
                 select 'XOLAIR' as brand,year, month, 'Paid Social' as channel, total_cost * 0.22 as cost from spend_data
                 union all
                 select 'XOLAIR' as brand,year, month, 'Third-Party Email' as channel, total_cost * 0.06 as cost from spend_data
                 union all
                 select 'XOLAIR' as brand,year, month, 'Point of Care' as channel, total_cost * 0.08 as cost from spend_data
                 union all
                 select 'XOLAIR' as brand,year, month, 'Custom' as channel, total_cost * 0.08 as cost from spend_data
                 union all
                 select 'XOLAIR' as brand,year, month, 'EHR/EMR' as channel, total_cost * 0.01 as cost from spend_data
                 ),
            channel_allocations_new as (
                 select 
                    'XOLAIR' as brand,
                     substring(cast(year_month as string), 1, 4) as year, 
                    substring(cast(year_month as string), 5, 2) as month, 
                     case 
                         when channel in ('POC') then 'Point of Care'
                         when channel in ('Digital Display') then 'Display'
                         when channel in ('3rd Party Email') then 'Third-Party Email'
                         when channel in ('Endemic Social') then 'Paid Social' 
                         when channel in ('Online Video', 'Video', 'Custom') then 'Custom'
                        end as channel,
                     sum(cost) as cost 
                from oasis_normalized_mars.mars_nvs_costs_unpivot_03182025_staging
                where audience = 'HCP'
                group by 1, 2, 3,4
            ) ,
            channel_allocations as (
                 select * from channel_allocations_old
                 union all
                 select * from channel_allocations_new
            ) ,
            
            monthly_reach as (
                 select
                    brand,
                     year,
                     month,
                     channel,
                     sum(reach) as total_monthly_reach
                 from all_hcp_unioned
                 group by brand, year, month, channel
            ) ,
                   
            final_allocation_including_missing_cost as (
                select 
                    coalesce(d.brand, ca.brand, mr.brand) as brand,
                    coalesce(d.channel, ca.channel, mr.channel) as channel,
                    coalesce(d.audience, 'HCP') as audience,
                    coalesce(d.year, ca.year, mr.year) as year,
                    coalesce(d.month, ca.month, mr.month) as month,
                     d.zip_code,
                     d.dma,
                     d.state,
                     d.country,
                     d.reach,
                     d.engage,
                     case 
                 when d.reach is not null and mr.total_monthly_reach is not null then (d.reach / mr.total_monthly_reach) * ca.cost 
                 else ca.cost 
             end as cost
                 from all_hcp_unioned d
                 inner join monthly_reach mr 
                      on d.year = mr.year and d.month = mr.month and d.channel = mr.channel
                 full join channel_allocations ca 
                      on coalesce(d.year,ca.year) = ca.year and coalesce(d.month,ca.month) = ca.month and coalesce(d.channel,ca.channel) = ca.channel 
                      where coalesce(d.year, ca.year, mr.year) = '2024'
                      and reach is null
            ) ,
            missing_cost_channel_year as (
            select 
            brand,channel,audience,year,sum(reach) as reach,sum(engage) as engage,sum(cost) as missing_cost 
            from final_allocation_including_missing_cost
            where  channel in ('Paid Social','Custom')
           group by 1,2,3,4
            ) ,
            
            final_allocation as (
                 select 
                     d.brand,
                     d.channel,
                     d.audience,
                     d.year,
                     d.month,
                     d.zip_code,
                     d.dma,
                     d.state,
                     d.country,
                     d.reach,
                     d.engage,
                     case when d.channel= ('Custom') and d.year='2024' and d.audience='HCP' then
                     (d.reach / mr.total_monthly_reach) * ca.cost + (d.reach / mr.total_monthly_reach) * (mc.missing_cost/11)
                     when 
                     d.channel= ('Paid Social') and d.year='2024' and d.audience='HCP' then
                     (d.reach / mr.total_monthly_reach) * ca.cost + (d.reach / mr.total_monthly_reach) * (mc.missing_cost/9)
                     else
                     (d.reach / mr.total_monthly_reach) * ca.cost end as cost
                 from all_hcp_unioned d
                 join monthly_reach mr 
                      on d.year = mr.year and d.month = mr.month and d.channel = mr.channel
                 join channel_allocations ca 
                      on d.year = ca.year and d.month = ca.month and d.channel = ca.channel
                left join missing_cost_channel_year mc 
                      on d.year = mc.year and d.channel = mc.channel and d.audience=mc.audience
             )
            select * from final_allocation
        """)
        df_hcp_all_cost_allocation.registerTempTable('hcp_all_channels')

        df_unioned = self.spark.sql("""
            with cte as(
                    select * from dtc_display
                    union all 
                    select * from dtc_paid_search
                    union all 
                    select * from dtc_poc
                    union all 
                    select * from dtc_paid_social
                    union all 
                    select * from hcp_paid_search
                    union all 
                    select * from hcp_all_channels
            )
                select * from cte 
            """)
        df_unioned.registerTempTable('combined_nvs_data')
        df_unioned_persisted = df_unioned.persist()
        audit_info = []
        audit_info += [{'table_name': f"{self.normalized_database}.mars_combined_nvs_data_staging",
                        'rows_updated': df_unioned_persisted.count()}]

        s3_path = (
                    "s3://" + self.bucket_name + "/" + self.normalized_folder + "/etl/nvs/mars_combined_nvs_data_staging/")
        df_unioned_persisted.coalesce(8).write.mode('overwrite').parquet(s3_path)
        self.s3_path_list += [s3_path]

        # Checking historical tables in normalized database
        paginator = athena_client.get_paginator('list_table_metadata')
        response_iterator = paginator.paginate(CatalogName='AwsDataCatalog', DatabaseName=self.normalized_database)

        all_table_names = []
        for page in response_iterator:
            all_table_names.extend((i['Name'] for i in page['TableMetadataList']))

        # Creating list of backup tables in normalized database
        existing_historical_table_names = []
        for tbl_name in all_table_names:
            if 'historical' in tbl_name:
                existing_historical_table_names.append(tbl_name)

        if 'mars_combined_nvs_data_historical' in existing_historical_table_names:
            historical_up_df = self.glue_context.create_data_frame.from_catalog(database=self.normalized_database,
                                                                                table_name='mars_combined_nvs_data_historical')
            last_version = historical_up_df.select([max("version")]).head()[0]
            hist_df = df_unioned_persisted.withColumn('version', lit(int(last_version) + 1))
        else:  # This means table is new to landing, it is getting loaded into backup for first time
            hist_df = df_unioned_persisted.withColumn('version', lit(1))
            logger.info(
                f"Back up table does not exist for mars_combined_nvs_data_historical in normalized layer")

        audit_info.append(
            {'table_name': f"{self.normalized_database}.mars_combined_nvs_data_historical",
             'rows_updated': df_unioned_persisted.count()})

        s3_path = ("s3://" + self.bucket_name + "/" + self.normalized_folder + "/etl/nvs/mars_combined_nvs_data_historical/")
        hist_df.coalesce(8).write.partitionBy("version").mode('append').parquet(s3_path)
        self.s3_path_list += [s3_path]

        return audit_info
    

    def create_update_crawler(self, client):
        """
        Crawler has to be created if it doesn't exist, if it exists then we have to update it s3 path for it.
        :param client: glue boto3 client used to create or update crawler
        """
        # Creating list of s3 path that has to be crawled
        s3_crawl_list = []
        for s3_path in list(set(self.s3_path_list)):
            s3_crawl_list.append(
                {"Path": s3_path, 'Exclusions': json.loads(self.config.get(self.env, "crawl_exclusion_paths"))})

        crawler_exist = True

        # Checking if crawler is already existing or not
        try:
            res = client.get_crawler(Name=self.crawler_name)
            response = json.dumps(res, indent=4, sort_keys=True, default=str)
            print(response)
        except client.exceptions.EntityNotFoundException as e:
            logger.info("Crawler doesnt exist, need to create")
            crawler_exist = False

        # Creating crawler if it doesn't exist
        if crawler_exist:
            response = client.update_crawler(
                Name=self.crawler_name,
                Role=self.config.get(self.env, "crawler_role"),
                DatabaseName=self.normalized_database,
                Targets={
                    'S3Targets': s3_crawl_list
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                LineageConfiguration={
                    'CrawlerLineageSettings': 'DISABLE'
                }
            )
            logger.info("Crawler Updated\nName : " + self.crawler_name)
            logger.info(json.dumps(response, indent=4, sort_keys=True, default=str))

        else:
            response = client.create_crawler(
                Name=self.crawler_name,
                Role=self.config.get(self.env, "crawler_role"),
                DatabaseName=self.normalized_database,
                Targets={
                    'S3Targets': s3_crawl_list
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                LineageConfiguration={
                    'CrawlerLineageSettings': 'DISABLE'
                }
            )
            logger.info("Crawler Created\nName : " + self.crawler_name)
            logger.info(json.dumps(response, indent=4, sort_keys=True, default=str))

    def start_crawler(self, client):
        """
        This method is used to start the crawler
        :param client: glue boto3 client used to create or update crawler
        """
        response_start = client.start_crawler(Name=self.crawler_name)
        logger.info("Crawler Started\nName : " + self.crawler_name)
        logger.info(json.dumps(response_start, indent=4, sort_keys=True, default=str))

    def monitor_crawler(self, client):
        """
        This method is used to monitor the crawler which we started just now
        :param client: glue boto3 client used to create or update crawler
        """
        need_to_wait = True
        state_previous = None
        while need_to_wait:
            time.sleep(20)
            response_get = client.get_crawler(Name=self.crawler_name)
            state = response_get["Crawler"]["State"]
            if state == "READY" or state == "STOPPING":  # Other known states: RUNNING
                logger.info(f"Crawler {self.crawler_name} is {state.lower()}.")
                need_to_wait = False
                break
            if state != state_previous:
                logger.info(f"Crawler {self.crawler_name} is {state.lower()}.")
                state_previous = state


def audit_job_info(spark_obj, job_srt_time, job_name, batch_id, audit_path, audit_dict):
    """
    This function is used to write audit stats of this etl job to athena table.
    :param audit_path:
    :param spark_obj:
    :param job_srt_time:
    :param job_name:
    :param batch_id:
    :param audit_dict:
    """
    if audit_dict is not None:
        logger.info("Writing completion stats to audit table")
        logger.info(audit_dict)
        audit_df = (spark_obj.createDataFrame(audit_dict)
                    .withColumn("log_id_status", lit("COMPLETED"))
                    .withColumn("script_exe_start_time", lit(job_srt_time))
                    .withColumn("script_exe_end_time", current_timestamp().cast(TimestampType()))
                    .withColumn("etl_script", lit(job_name))
                    .withColumn("etl_layer", lit("normalized"))
                    .withColumn("batch_id", lit(batch_id)))
        audit_df.write.mode('append').parquet(audit_path)
        logger.info("Auditing is complete")
    else:
        logger.info("Writing initiating stats to audit table")
        audit_df = (spark_obj.createDataFrame([{'etl_layer': 'normalized', 'log_id_status': 'INITIATED'}])
                    .withColumn("script_exe_start_time", lit(job_srt_time))
                    .withColumn("script_exe_end_time", lit(None).cast(TimestampType()))
                    .withColumn("etl_script", lit(job_name))
                    .withColumn("table_name", lit(None).cast(StringType()))
                    .withColumn("rows_updated", lit(None).cast(IntegerType()))
                    .withColumn("batch_id", lit(batch_id)))
        audit_df.write.mode('append').parquet(audit_path)


if __name__ == "__main__":
    # Getting AWS Glue job parameters, creating session and initiating job
    arg = getResolvedOptions(sys.argv,
                             ['JOB_NAME', 'environment', 'batch_id'])

    logger.info("MARS XO job started")

    environment = arg['environment']
    config_obj = read_config_file('configuration.ini')
    check_config_section(config_obj, environment)
    job_start_time = datetime.datetime.now()

    sc = SparkContext()
    glueContext = GlueContext(sc)
    sprk = glueContext.spark_session
    job = Job(glueContext)
    job.init(arg['JOB_NAME'], arg)

    audit_job_info(sprk, job_start_time, arg['JOB_NAME'], arg['batch_id'],
                   audit_path=config_obj.get(environment, 'audit_path'),
                   audit_dict=None)
    audit_list = []

    nvs_etl = NvsETL(config_obj, arg, glueContext, sprk)

    crawler_client = boto3.client('glue', region_name="us-west-2")

    athena_cl = boto3.client('athena', region_name="us-west-2")

    audit_list += nvs_etl.nvs_tam(athena_cl)

    audit_list += nvs_etl.nvs_digital(athena_cl)

    nvs_etl.create_update_crawler(crawler_client)

    nvs_etl.start_crawler(crawler_client)

    nvs_etl.monitor_crawler(crawler_client)

    audit_job_info(sprk, job_start_time, arg['JOB_NAME'], arg['batch_id'],
                   audit_path=config_obj.get(environment, 'audit_path'),
                   audit_dict=audit_list)

    job.commit()
