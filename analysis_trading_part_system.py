import pandas, psycopg2, requests, json


#bring up relevant stock data and put into a pandas dataframe
con = psycopg2.connect(database="***", user="***", password="***", host="***", port="***")
print("Database opened successfully")

cur = con.cursor();
cur.execute('''SELECT * FROM STOCK_TICKER; ''');
store1 = cur.fetchall();
df = pandas.DataFrame(store1,columns = ['timestamp','open','high','low','close','volume'])
print(df.to_string(index=False,justify='left-justify'))

con.close()

#calculate average of last ten closing prices
av = df['close'][-10:].mean()
av = float(av)

#store the last closing price
l_day = df['close'][-1:].to_string(index=False)
l_day = float(l_day)

#two order objects are made according to Alpaca api rules
data = {
        "symbol":"STOCK_TICKER",
        "qty":100,
        "side":"buy",
        "type":"market",
        "time_in_force":"gtc"
        }
data1 = {
        "symbol":"STOCK_TICKER",
        "qty":100,
        "side":"sell",
        "type":"market",
        "time_in_force":"gtc"
        }
#url for accessing API and necessary keys
url2 = "https://paper-api.alpaca.markets/v2/orders"
header_info={'APCA-API-KEY-ID':"***", 'APCA-API-SECRET-KEY':"***"}


#a simple moving average strategy implementation based on last ten closing prices and last
# closing price
if (l_day > av):
    r = requests.post(url2,json = data, headers = header_info)
    print (json.loads(r.content))
    
else:
    b = requests.post(url2,json = data1, headers = header_info)
    print (json.loads(b.content))