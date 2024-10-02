from pyspark.sql import SparkSession
import logging.config


logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Create_spark')



def sparksession(envn , appname):
    try:
        loggers.info('sparkSession method started')
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'
        loggers.info(f'master is {master}')

        spark = SparkSession.builder.master(master).appName(appname).getOrCreate()

        
    

    except Exception as e:
        loggers.error(' An err occur in sparksession obj :', str(e))
        raise
    else:
        loggers.info('Spark obj created.')
    
    
    return spark