from kafka import KafkaConsumer
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import ElasticNet
from joblib import load
from sklearn.model_selection import train_test_split
import datetime
import threading
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from sklearn.metrics import mean_absolute_error as mae
from pyarrow import Table
from pyarrow.fs import HadoopFileSystem
import yfinance as yf
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings("ignore")

print("starting consumer...")
consumer = KafkaConsumer(
    'stock_data1',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Start reading at the beginning of the topic , put latest to get latest data
    enable_auto_commit=True,
    group_id='my-group'
)
def show_loading():
    while loading:
        print("Loading data...", end="\r")
        time.sleep(0.5)  # Sleep to avoid high CPU usag
for i in consumer:
    loading = True
    loading_thread = threading.Thread(target=show_loading)
    loading_thread.start()
    df = pickle.loads(i.value)
    print("Data picked from the broker\n")
    loading = False
    loading_thread.join()
    #hdfs_path = f'hdfs://localhost:9000/user/vivek/stream/{df.index.name}.parquet'
    #table = pa.Table.from_pandas(df)
    #pq.write_table(table, hdfs_path)    
    #print("Data saved in hdfs path:localhost:9000/user/vivek/stream")
    print(df.index.name)
    df['Date']=df['Date'].dt.date
    df['date_l']=df['Date']
    df['open_l'] = df['Open'].shift(1)
    df['high_l']= df['High'].shift(1)
    df['low_l']= df['Low'].shift(1)
    df['close_l']= df['Close'].shift(1)
    df['volume_l']= df['Volume'].shift(1)
    print("Want to include dividends{Y/n}: ")
    d = input()
    flag =0
    while(d != 'Y' and d !='n'):
        print('please enter valid response')
        d=input()
    if (d == 'Y'):
        df['dividends_l']= df['Dividends'].shift(1)
        df=df.dropna()
        print("dividends included.\n")
        x = df[['date_l','open_l','high_l','low_l','close_l','volume_l','dividends_l']]
        flag=4
    else:
        print("ok\n")
        df=df.drop('Dividends',axis=1)
        flag =1 
        print("Want to include stock_splits{Y/n}: ")
    print("Want to include stock splits{Y/n}: ")
    d = input()
    while(d != 'Y' and d !='n'):
        print('please enter valid response')
        d=input()
    if (d == 'Y'):
        df['stock_splits_l']= df['Stock Splits'].shift(1)
        df=df.dropna()
        print("Stock Splits included.\n")
        if(flag==4):
            x=df[['date_l','open_l','high_l','low_l','close_l','volume_l','dividends_l','stock_splits_l']]
        else:
            x = df[['date_l','open_l','high_l','low_l','close_l','volume_l','stock_splits_l']]
    else:
        print("ok\n")
        df=df.drop('Stock Splits',axis=1)
        flag =2
    if(flag==1 and flag==2 ):
        x = df[['date_l','open_l','high_l','low_l','close_l','volume_l']]
    y = df['High']
    l = list()
    for i in x['date_l']:
        l.append(int(i.strftime("%Y%m%d")))
    x['date_l']=l

    ss = StandardScaler()
    xx = ss.fit_transform(x)
    seed = 7
    if(df.index.name=='AAPL'):
        model = load("model.joblib") # for apple, i pre-trained the model  
    else:
        model=ElasticNet(alpha=1.0, l1_ratio=0.5, random_state=seed)
    x_train,x_test,y_train,y_test= train_test_split(xx,y,test_size=0.33)
    model.fit(x_train,y_train)
    y_pred = model.predict(x_test)
    ee = mae(y_pred,y_test)
    ee1=r2_score(y_test,y_pred) 
    #print(ee1)
    if(ee1>0.6 ):
        print("Created Robust model\n")
    else:
        print("Model might be inaccurate\n")
    stock = yf.Ticker(df.index.name)
    data = stock.history(period='1d') # latest data
    data = data.reset_index()
    if(flag==1):
        data=data.drop('Dividend',axis=1)
    if(flag==2):
        data=data.drop('Stock Splits',axis=1)
    data['Date']=datetime.date.today().strftime("%Y%m%d")
    print(data)
    xf = ss.fit_transform(data) 
    pred = model.predict(xf)
    print("Tommarow's High predicted is {}".format(pred))
    print("press ctrl+c to exit.")

#pip install hdfs3
#pip install scikit-learn
#pip install joblib