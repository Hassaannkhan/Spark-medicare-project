from pyspark.sql.functions import *
from pyspark.sql.types import *
from validate import dropnull
def data_clean( df1 , df2 ): 
    global df_city_sel , df_presc_sel , df_presc_clean
    try:
        print('Data cleaning method running.')
        df_city_sel = df1.select(
                                upper(df1.city).alias('city'),
                                df1.state_id,
                                upper(df1.state_name).alias('state_name'),
                                upper(df1.county_name).alias('county_name'),
                                df1.population,
                                df1.zips
                                )

        print('working on OLTP dataset ')

        df_presc_sel = df2.select(
            df2.npi.alias('presc_id'),
            df2.nppes_provider_last_org_name.alias('presc_lname'),
            df2.nppes_provider_first_name.alias('presc_fname'),
            df2.specialty_description.alias('presc_speclty'),
            df2.drug_name,
            df2.total_drug_cost,
            df2.total_day_supply,
            df2.years_of_exp,
            df2.total_claim_count
            )
        
        print('adding new col to df_presc_sel..')

        df_presc_sel = df_presc_sel.withColumn('Country_name' , lit('USA'))

        print('converting years_of_exp from STR to INT')

        df_presc_sel = df_presc_sel.withColumn('years_of_exp' , regexp_replace(col('years_of_exp') , r"^=" , ''))
        df_presc_sel = df_presc_sel.withColumn('years_of_exp' , col('years_of_exp').cast(DoubleType()))

        print('concat fname & lname')

        df_presc_sel = df_presc_sel.withColumn('full_name' , concat_ws(" ", 'presc_lname' , 'presc_fname')) 

        print('DATA AFTER PROCESSING')

        df_presc_sel = df_presc_sel.drop('presc_lname' , 'presc_fname')

        df_presc_sel = df_presc_sel.dropna(subset='presc_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')
        df_presc_clean = dropnull(df_presc_sel)
         
        

        # df_presc_sel = df_presc_sel.select([count(when(isnan(c) | col(c).isNull() , c)).alias(c) for c in df_presc_sel.columns])

        # print('Successfully drop NaN & Nulls')

        # df_presc_sel = df_presc_sel.select([count(when(isnan(c) | col(c).isNull() , c)).alias(c) for c in df_presc_sel.columns])
        


    except Exception as e:
        print('err occur in data clean..', str(e))

    else:
        print('data_clean method executed')
        return  df_presc_sel  , df_city_sel 




