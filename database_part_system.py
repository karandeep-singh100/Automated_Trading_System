import pandas
import requests
import csv

import psycopg2
from psycopg2 import extensions



#access alphavantage API and print out the data line by line.
with requests.Session() as s:
    download = s.get("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=5min&apikey=***&datatype=csv")
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines())
    my_list = list(cr)
    for row in my_list:
        print(' '.join(row))
"""
create a postgreSQL connection, change isolation level to do a command and
check for a database.
At the end, isolation level is set to old value.
"""
con = psycopg2.connect(database="***", user="***", password="***", host="***", port="***")
print("Database opened successfully")


print ("isolation_level:", con.isolation_level)
old_con = psycopg2.extensions.ISOLATION_LEVEL_DEFAULT

con.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)


cur = con.cursor()
cur.execute("SELECT datname FROM pg_database;")
list_database = cur.fetchall()

database_name = input('Enter database name to check exist or not: ')
if (database_name.lower(),) in list_database:
    print("'{}' Database already exist".format(database_name))
else:
    print("'{}' Database not exist.".format(database_name))
    sql = '''CREATE database ***''';
    cur.execute(sql)
    print('new made')

con.set_isolation_level(old_con)
print ("isolation_level:", con.isolation_level)

print('Done')
con.close()
"""
create a postgreSQL connection, create a table, add in data, access all that
data and print it out
"""
con = psycopg2.connect(database="***", user="***", password="***", host="***", port="***")
print("Database opened successfully")

cur = con.cursor();

sqlone='''CREATE TABLE msft(timdat timestamp PRIMARY KEY,
open NUMERIC,
high NUMERIC,
low NUMERIC,
close NUMERIC,
volume BIGINT )''';
cur.execute(sqlone);

cr1 = csv.reader(decoded_content.splitlines())
next(cr1)
for row in cr1:
    cur.execute("INSERT INTO msft VALUES (%s, %s, %s, %s, %s, %s)", row)

cur.execute('''SELECT * FROM msft; ''');
store1 = cur.fetchall();
df = pandas.DataFrame(store1,columns = ['timestamp','open','high','low','close','volume'])
print(df.to_string(index=False,justify='left-justify'))

con.commit()
con.close()