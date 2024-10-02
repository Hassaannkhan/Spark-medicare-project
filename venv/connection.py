import mysql.connector

def connection(host , user , password , db):
    
    try:
        print('connection method is running')
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            auth_plugin='mysql_native_password'
        )

        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        print('DB crreated successfully')

    except Exception as e:
        print(f'error occur during connection {str(e)}')

    else:
        cursor.close()
        connection.close()
