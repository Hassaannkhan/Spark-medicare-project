

def load(df1 , df2 , host , port , db , user , password):
    try:
        print('load method running')
        mysql_url = f"jdbc:mysql://{host}:{port}/{db}"  # replace testdb with your database name
        mysql_properties = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Write DataFrame to MySQL
        df1.write.jdbc(
            url=mysql_url,
            table="us_cities_dim", 
            mode="overwrite",
            properties=mysql_properties
        )

        df2.write.jdbc(
            url=mysql_url,
            table="usa_presc_medicare",  
            mode="overwrite",  
            properties=mysql_properties
        )

    except Exception as e:
        print(f'error occur in load method {str(e)}')
    
    else:
        print("Data saved successfully!")