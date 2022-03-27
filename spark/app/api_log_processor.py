import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import levenshtein, udf, explode, least, greatest, concat
from modules.storage_service import StorageService
from utils.logging import get_logger
import uuid

# Create spark session
spark = SparkSession \
    .builder \
    .getOrCreate()

sc = spark.sparkContext
logger = get_logger(sc.appName)
try:
    ####################################
    # Parameters
    ####################################

    source_folder = sys.argv[1]
    target_folder = sys.argv[2]
    max_session_length = 60
    max_input_diff = 7

    ####################################
    # Read raw Data
    ####################################
    logger.debug(f'source_folder: {source_folder}, {target_folder} ')

    st_ser = StorageService(spark)
    api_schema = StructType([
        StructField('api_timestamp', TimestampType(), True),
        StructField('api_key', StringType(), True),
        StructField('input', StringType(), True)])

    key_cols = ['api_key']
    api_log_df = st_ser.get_df_from_file(spark, api_schema, key_cols, source_folder)
    logger.debug(f'api_log_df count: {api_log_df.count()}')

    uuid_udf = udf(lambda : str(uuid.uuid4().hex), StringType())
    api_log_df.createOrReplaceTempView("api_log")

    agg_log_df = spark.sql(
        f"""
        -- aggregate inputs and timestamps
        SELECT api_key,session_flag
            , min(api_timestamp) as session_start
            , max(api_timestamp) as session_end
            , collect_list(input) as inputs
        FROM (
            SELECT *
            -- determine sessioned inputs within batch
            , count(CASE WHEN bigint(to_timestamp(api_timestamp)) - bigint(to_timestamp(prev_timestamp)) > {max_session_length} 
                        OR levenshtein(input, prev_input) > {max_input_diff}
                        OR prev_timestamp is NULL THEN 1
                    ELSE NULL END
                ) OVER(PARTITION BY api_key ORDER BY api_timestamp)
            AS session_flag
            FROM (
                -- get previous timestamp and input for every log record
                SELECT api_key, input, api_timestamp
                    , LAG(input) OVER(partition by api_key ORDER BY api_timestamp) prev_input
                    , LAG(api_timestamp) OVER(partition by api_key ORDER BY api_timestamp) prev_timestamp
                FROM api_log
            )
        )
        GROUP BY api_key, session_flag
        """
    )
    agg_log_df = agg_log_df.withColumn('session_key', uuid_udf())
    agg_log_df = agg_log_df.drop('session_flag')
    logger.debug(f'agg_log_df count: {agg_log_df.count()}')

    ####################################
    # Read Session Data
    ####################################
    session_schema = StructType([
        StructField('session_key', StringType(), True),
        StructField('session_start', TimestampType(), True),
        StructField('session_end', TimestampType(), True),
        StructField('api_key', StringType(), True),
        StructField('inputs', ArrayType(StringType()), True)])

    if not st_ser.path_exists(target_folder):
        logger.debug('Session data does not exist, insert only')
        new_sessions_df = agg_log_df
    else:
        session_df = st_ser.get_df_from_file(spark, session_schema, key_cols, f'{target_folder}/*', target_folder)
        log_part_df = agg_log_df.select(*key_cols).distinct()
        session_df = session_df.join(log_part_df, log_part_df.api_key==session_df.api_key, 'leftsemi')
        session_df = session_df.cache()
        logger.debug(f'session_df count: {session_df.count()}')

    ####################################
    # Get New Sessions
    ####################################

        overlap_session_condition = [session_df.api_key ==  agg_log_df.api_key
                , 
                (agg_log_df.session_start.between(session_df.session_start, session_df.session_end)
                | agg_log_df.session_end.between(session_df.session_start, session_df.session_end)
                | session_df.session_end.between(agg_log_df.session_start, agg_log_df.session_end)
                | (session_df.session_start.cast("long") - agg_log_df.session_end.cast("long")).between(0, max_session_length)
                | (agg_log_df.session_start.cast("long") - session_df.session_end.cast("long")).between(0, max_session_length)
                )
            ]

        new_sessions_df = agg_log_df.join(
            session_df
            ,  overlap_session_condition
            , "leftanti"
        )
        logger.debug(f'new_sessions_df count: {new_sessions_df.count()}')

    ####################################
    # Get Overlap Sessions
    ####################################

        overlap_sessions_df = agg_log_df.join(
            session_df
            ,  overlap_session_condition
            , "inner"
        )
        overlap_sessions_df = overlap_sessions_df.cache()

        # Get existing session records where inputs are added by current batch
        upd_session_df = overlap_sessions_df \
            .withColumn('old_input', explode(session_df.inputs)).withColumn('new_input', explode(agg_log_df.inputs)) \
            .where(levenshtein('old_input', 'new_input') <= max_input_diff) \
            .dropDuplicates(['session_key']) \
            .select(
                session_df.session_key,
                agg_log_df.session_key.alias('session_key_new'),
                least(session_df.session_start, agg_log_df.session_start).alias('session_start'),
                greatest(session_df.session_end, agg_log_df.session_end).alias('session_end'),
                concat(session_df.inputs, agg_log_df.inputs).alias('inputs'),
                session_df.api_key
        )

        # Process new records
        overlap_sessions_df = overlap_sessions_df.select(agg_log_df['*'])
        new_inputs_df = overlap_sessions_df.join(
            upd_session_df
            , [upd_session_df.api_key==overlap_sessions_df.api_key, upd_session_df.session_key_new==overlap_sessions_df.session_key]
            , "leftanti"
        )
        logger.debug(f'new_inputs_df count: {new_inputs_df.count()}')
        upd_session_df = upd_session_df.drop('session_key_new')
        new_sessions_df = new_sessions_df.union(new_inputs_df)


        # Get session partitions to overwrite
        part_df = upd_session_df.select(*key_cols).distinct()
        upd_partitions_df = session_df.join(part_df, part_df.api_key==session_df.api_key, 'leftsemi')
        # Remove outdated records
        upd_partitions_df = upd_partitions_df.join(
            upd_session_df
            , [upd_session_df.api_key==upd_partitions_df.api_key, upd_session_df.session_key==upd_partitions_df.session_key]
            , 'leftanti'
        )
        # Overwrite updated partitions
        upd_partitions_df = upd_session_df.union(upd_partitions_df)
        logger.debug(f'upd_partitions_df count: {upd_partitions_df.count()}')

    ####################################
    # Load data to Storage
    ####################################

        st_ser.save_df_to_file(upd_partitions_df, target_folder, key_cols, 'overwrite')
    st_ser.save_df_to_file(new_sessions_df, target_folder, key_cols, 'append')
except Exception as e:
    logger.error(f'Failed processing raw data with args {sys.argv}:\n{e}')
    raise e
