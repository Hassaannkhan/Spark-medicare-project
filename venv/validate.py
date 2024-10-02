import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Validate')

def curr_date(spark):
    try:
        logging.warning('started curr_date method ')
        out = spark.sql("""select current_date""")
        logger.warning(f"validating spark obj with curr date {out.collect()}")

    except Exception as e:
        logger.error('err occured in curr_date',str(e))
        raise

    else:
        logger.warning('Validation done')
        return out.collect()

    
def dropnull(df1):
    try:
        print('running dropnull function...')    
        df_clean = df1.select([count(when(isnan(c) | col(c).isNull() , c)).alias(c) for c in df1.columns])
    except Exception as e:
        print(f'error occur in dropnull function : str({e})')
    else:
        print('Successfully run dropnull function')
        return df_clean
      
