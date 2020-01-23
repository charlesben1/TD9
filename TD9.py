import requests
import json
import decimal
import hmac
import time
import sqlite3
import hashlib




binance_keys = {
    'api_key': "api key",
    'secret_key': "secret key"
}

class Binance:
    def __init__(self):

        self.base = 'https://api.binance.com'

        self.endpoints = {
            "order": '/api/v3/order',
            "testOrder": '/api/v3/order/test',
            "allOrders": '/api/v3/allOrders',
            "klines": '/api/v3/klines',
            "exchangeInfo": '/api/v3/exchangeInfo'
        }

        self.headers = {"X-MBX-APIKEY": binance_keys['api_key']}


    def getCurrencies(self, quoteAssets:list=None):

        url = self.base + self.endpoints["exchangeInfo"]

        try:
            response = requests.get(url)
            data = json.loads(response.text)
        except Exception as e:
            print("Exception occured when trying to access "+url)
            print(e)
            return []

        symbols_list = []

        for pair in data['symbols']:
            if pair['status'] == 'TRADING':
                if quoteAssets != None and pair['quoteAsset'] in quoteAssets:
                    symbols_list.append(pair['symbol'])

        return(symbols_list)

    def getDepth(self, symbol:str, interval:str):

        params = '?&symbol=' + symbol + '&interval='+ interval

        url = self.base + self.endpoints["klines"] + params


        data = requests.get(url)
        dictionary = json.loads(data.text)

        df = pd.DataFrame.from_dict(dictionary)
        df = df.drop(range(6, 12), axis=1)

        col_names = ['time', 'open', 'high', 'low', 'close', 'volume']
        df.columns = col_names

        for col in col_names:
            df[col] = df[col].astype(float)

        df['date'] = pd.to_datetime(df['time'] * 1000000, infer_datetime_format=True)

        return df

    def sqlConnection():
        try:
            con = sqlite3.connect('datas.db')
            return con
        except Error:
            print(Error)

    def createCandleTable(con, asset, duration):
        cursorObj = con.cursor()
        setTableName = str("Binance_" + asset + "_"+ duration)
        tableCreationStatement = """CREATE TABLE """ + setTableName + """(opentime
        INT, open REAL, high REAL, low REAL, close REAL,volume REAL, closetime int, quotevolume
        REAL, nbtrades REAL, takerbuybaseassetvolume REAL, takerbuyquoteassetvolume REAL, ignore REAL)"""
        cursorObj.execute(tableCreationStatement)
        con.commit()
        con.close()

    def createTradeTable(con, asset):
        cursorObj = con.cursor()
        setTableName = str("Binance_" + asset)
        tableCreationStatement = """CREATE TABLE """ + setTableName + """(aggTradeId INT, price REAL,
        quantity REAL, firstTradeId INT, lastTradeId INT, timestamp INT, buyerMaker BOOL, tradeBestPriceMatch BOOL)"""
        cursorObj.execute(tableCreationStatement)
        con.commit()
        con.close()



    def createOrder(api_key, secret_key, direction, price, amount, asset, orderType):
    params = {
        'symbol' : asset,
        'price' : price,
        'quantity' : amount,
        'side' : direction,
        'type' : orderType,
        'timeInForce' : 'GTC',
        'recvWindow' : 5000,
        'timestamp' : int(time.time() * 1000 - 5000)
    }
    params['signature'] = hmac.new(secret_key.encode('utf-8'), urllib.parse.urlencode(params).encode('utf-8'), hashlib.sha256).hexdigest()
    r = requests.post(self.base + self.endpoints["order"], headers = {'X-MBX-APIKEY': api_key}, params = params).json()
    print(r)


    def cancelOrder(api_key, secret_key, asset, uuid):
        params = {
            'symbol' : asset,
            'orderId' : uuid,
            'recvWindow' : 5000,
            'timestamp' : int(time.time() * 1000 - 5000)
        }
        params['signature'] = hmac.new(secret_key.encode('utf-8'), urllib.parse.urlencode(params).encode('utf-8'), hashlib.sha256).hexdigest()
        r = requests.delete(self.base + self.endpoints["order"], headers = {'X-MBX-APIKEY': api_key}, params = params).json()
        print(r)
