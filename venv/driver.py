import env_var as env
from spark_object import sparksession
from validate import curr_date
import logging
import logging.config
import sys , os
from ingest import load_file , display_df , df_count
from data_processing import data_clean
from connection import connection
from load import load

logging.config.fileConfig('Properties/configuration/logging.config')



def main():
    global file_dir , file_format , header , inferSchema , header2 , inferSchema2 , df_fact
    try:
        logging.info('-------MAIN--------')
        # print(env.appName)
        logging.info('calling spark obj')
        spark = sparksession(env.envn , env.appName)

        logging.info('validating spark obj..........')
        curr_date(spark)


        for file2 in os.listdir(env.src_oltp):
            print(file2)

            file_dir2 = env.src_oltp + '\\' + file2 

            if file2.endswith('.csv'):
                file_format = 'csv'
                header2 = env.header
                inferSchema2 = env.inferSchema

            elif file2.endswith('.parquet'):
                file_format = 'parquet'
                header2 = 'NA'
                inferSchema2 = 'NA'
            

            logging.info('reading file wich is of > {}'.format(file_format))

            df_fact  = load_file(spark=spark , file_dir=file_dir2, file_format=file_format , header=header2, inferSchema=inferSchema2)
            logging.info('displaying df {}'.format(df_fact))
            display_df(df_fact , 'df_fact')

            logging.info('validating df')
            df_count(df_fact)




        for file in os.listdir(env.src_olap):
            print(file)

            file_dir = env.src_olap + '\\' + file 

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferSchema = env.inferSchema

            logging.info('reading file wich is of > {}'.format(file_format))

            df_city  = load_file(spark=spark , file_dir=file_dir, file_format=file_format , header=env.header, inferSchema=inferSchema)
            logging.info('displaying df {}'.format(df_city))
            display_df(df_city , 'df_city')

            logging.info('validating df')
            df_count(df_city)



        print('---implementing data processing methods')

        var1 , var2  = data_clean(df_city , df_fact)

        display_df(var1)
        
        display_df(var2)


        connection(env.host , env.user , env.password , env.db )
        

        load(var1 , var2 , env.host , env.port , env.db , env.user , env.password)
        print('--DATA LOAD SUCCESSFULLY--')

        

    except Exception as e:
        logging.error('An error occured when calling main(), check trace : ' , str(e))
        sys.exit(1)

if __name__ == '__main__':

    main()
    logging.info('Application DONE')
    sys.exit(0)



