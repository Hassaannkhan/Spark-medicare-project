import os


os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'PySpark_Project'

curr = os.getcwd()

src_olap = curr + '\src\olap'
src_oltp = curr + '\src\oltp'

host = '127.0.0.1'
user= ''
port  = '3306'
db = 'USA_Medicare'
password = ''
