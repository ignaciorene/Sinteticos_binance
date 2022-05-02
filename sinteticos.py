#INSTALO LIBRERIAS
import pip

packages=['websocket-client','datetime','pandas']

def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])  

for package in packages:
    import_or_install(package)

### IMPORTO LIBRERIAS
from websocket import create_connection
import json
from datetime import datetime, timedelta
import pandas as pd

print('')
print('STARTING...')

## OBTENGO LAS FECHAS
current_date = datetime.now()
current_quarter = round((current_date.month - 1) / 3 + 1)
last_date = datetime(current_date.year, 3 * current_quarter + 1, 1)+ timedelta(days=-5)
remaining_days=last_date-current_date

#### PIDO LOS DATOS A BINANCE
tickets=['btcusdt','ethusdt','adausdt','linkusdt','bchusdt','dotusdt','xrpusdt','ltcusdt','bnbusdt']
tickets_futures=['btcusd','ethusd','adausd','linkusd','bchusd','dotusd','xrpusd','ltcusd','bnbusd']
spot_prices=[]
futures_prices=[]

def data_spot(message):
    json_message=json.loads(message)
    pair=json_message['s']
    last_price=json_message['c']
    print(f'{pair} = {last_price}')
    spot_prices.append(float(last_price))

def data_futures(message):
    json_message=json.loads(message)
    pair=json_message['ps']
    end_date=last_date.strftime('%Y%m%d')
    last_price=json_message['k']['c']
    print(f'{pair}_{end_date} = {last_price}')
    futures_prices.append(float(last_price))

print('GETTING SPOT PRICES...')
print('')
for ticket in tickets:
    socket= f'wss://stream.binance.com:9443/ws/{ticket}@ticker'
    ws = create_connection(socket)
    data_spot(ws.recv())
ws.close()

print('')
print('GETTING FUTURES PRICES...')
print('')
for ticket in tickets_futures:
    socket=f'wss://dstream.binance.com/ws/{ticket}_next_quarter@continuousKline_1m'
    ws = create_connection(socket)
    data_futures(ws.recv())
ws.close()

##### CREO EL DATAFRAME Y EL EXCEL
print('')
print('CREATING EXCEL...')
print('')

data={
    'ticket':tickets,
    'spot_price':spot_prices,
    'future_price':futures_prices,
    'end_contract':last_date,
    'remaining_days':remaining_days
}

df=pd.DataFrame(data)

def get_tna(future_price,spot_price):
    future=float(future_price)
    spot=float(spot_price)
    return (((future/spot)-1)/float(remaining_days.days))*365

df['TNA']=df.apply(lambda x: get_tna(x['future_price'],x['spot_price']),axis=1)

print(df)
print('')

df.to_excel('sinteticos_binance.xlsx', index=False)

print('FINISHED')
