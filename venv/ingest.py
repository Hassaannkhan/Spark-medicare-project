
import logging.config
logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Ingest')

def load_file(spark , file_dir, file_format, header, inferSchema):
    try:
        logger.warning('Load_file method starts')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).options(header=header).options(inferSchema=inferSchema).load(file_dir)

        
    except Exception as e:
        logger.error('An err occur at load_file', str(e))
        raise
    else:
        logger.warning('df created sucessfully which is of {}'.format(file_format))


    return df


def display_df(df):
    df_show = df.show(2)
    return df_show




def df_count(df):
    global df_c
    try:
        logger.warning('record count {}'.format(df))
        df_c = df.count()

    except Exception as e:
        raise
    else:
        logger.warning('number of records are :: {}'.format(df_c))
        print('-------- number of records are :: {}'.format(df_c))
        return df_c
