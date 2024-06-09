import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta
import threading
import time
from kafka import KafkaProducer
import pickle


print("Enter the stock code for which you want to predict the high: \n"
               "eg:AAPL (apple.inc)\n"
                "   NVDA (nvidia)\n"
                "   RELIANCE.NS (reliance)\n"
                "   ADANIPOWER.NS (adani_power)")
symbol = input()

print("Enter number of months of past data you want to load and train: ")
mo = int(input())
def show_loading():
    while loading:
        print("Loading the past {} months data for {} ....".format(mo,symbol), end="\r")
        time.sleep(0.5)  # Sleep to avoid high CPU usag
loading = True
loading_thread = threading.Thread(target=show_loading)
loading_thread.start()
stock = yf.Ticker(symbol)
datem = datetime.date.today() - relativedelta(months=mo)
df = stock.history(start = datem, end = datetime.date.today())
df = df.reset_index()
loading = False
loading_thread.join()
print("\nDone\n")

producer = KafkaProducer(bootstrap_servers='localhost:9092')
df.index.name=symbol 
#print(df)
data_bytes = pickle.dumps(df)
producer.send('stock_data1',data_bytes)
producer.flush()
print("Data sent to the broker of the topic 'stock_data', success...\n")








#install necessary libraries in your system
#pip install kafka-python
#pip install yfinance
#pip install scikit-learn
#pip install joblib