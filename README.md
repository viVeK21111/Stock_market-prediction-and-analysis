# Analysis and prediction of stock data 
-> In this project i used yfinance module in python to fetch the latest and historic data of stock
-> Then for apple stock i pretrained the model with last 6 months data and builded a model 

# kakfa
-> Then used kafka , producer to fetch the data and send to the topic stock_data in broker running on localhost:9890
-> using consumer to fetch the data from the broker and do some basic analysis without preprocessing much and diretly trained with Elasticnet regression model
![alt text](http://cloudurable.com/images/kafka-architecture-topics-producers-consumers.png)

# prediction
->prediction of high based on the latest value of open,close,high,volume,dividends and stock splits
->applied shift() method for the data , to train the previous data for the present date so we can predict the high for current date and previous day data
->trained ml model is saved using joblib. (/model.joblib) 

--> for more insights check the example for AAPL (apple.inc) stock in /temp.ipynb

# how to run
--> to run the project, first execute producer.py and then consumer.py with kafka,hadoop and necessary python libraries installed on your linux os or you can use aws ec2 with linux os.
